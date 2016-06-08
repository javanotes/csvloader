/* ============================================================================
*
* FILE: AsciiFileReader.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactivetechnologies.csvloader.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.logging.Logger;
/**
 * A byte stream based reader. The fetching of bytes is performed in a separate thread than the reader thread.
 * This class is not thread safe for multiple readers. The communication between fetch thread and reader thread is via a {@linkplain SynchronousQueue}. 
 * This class will never buffer more characters than what present in a single line of record, and thus should be 
 * memory efficient than a standard {@linkplain BufferedReader}. <p><b>NOTE:</b> Though this class extends {@linkplain Reader}, it does not implement
 * the {@link #read(char[], int, int) read} method and attempt to invoke the method would fail. Use {@link #readLine()} to read lines of text.
 */
public class AsciiFileReader extends Reader implements Runnable{

  /*
   *  A line is considered to be terminated by any one of a line feed ('\n'), a carriage return ('\r'), 
   *  or a carriage return followed immediately by a line feed.
   */
  final static byte CARRIAGE_RETURN = 0xD;
  final static byte LINE_FEED = 0xA;
  private static final Logger log = Logger.getLogger(AsciiFileReader.class.getSimpleName());
  
  private byte[] lineBytesAccumulated;
  private final AbstractFileChunkHandler fileReader;
  private final Thread fetchThread;
  /**
   * New reader instance.
   * @param file the file to read
   * @param memMappedIO whether to use mapped byte buffer
   * @throws IOException
   */
  public AsciiFileReader(File file, boolean memMappedIO) throws IOException {
    super();
    this.fileReader = memMappedIO ? new MemoryMappedChunkHandler(file) : new ByteChannelChunkHandler(file, 8192);
    fetchThread = new Thread(this);
    fetchThread.start();
  }
  /**
   * New reader instance using memory mapped IO.
   * @param file the file to read
   * @throws IOException
   */
  public AsciiFileReader(File file) throws IOException {
    this(file, true);
  }
  private final SynchronousQueue<byte[]> line = new SynchronousQueue<>();
  
  private static boolean isEOF(byte[] bytes)
  {
    return bytes.length == 1 && bytes[0] == -1;
  }
  
  /**
   * Reads a line of text. A line is considered to be terminated by any one of a line feed ('\n'), a carriage return ('\r'), 
   * or a carriage return followed immediately by a linefeed.
   * @return A String containing the contents of the line, not including any line-termination characters, 
   * or null if the end of the stream has been reached
   * @throws IOException
   */
  public String readLine() throws IOException
  {
    if(streamComplete)
      throw new IOException("End of stream");
    
    byte[] bytes = null;
    try {
      bytes = line.take();
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
    if(bytes == null)
      throw new IOException(new IllegalStateException("Fetched null bytes"));
    if(isEOF(bytes))
      return null;
    
    return new String(bytes, StandardCharsets.UTF_8);
    
  }
  private volatile boolean streamComplete;
  private void fetch() throws IOException
  {
    FileChunk chunk = fileReader.readNext();
    while (chunk != null) {
      splitBytes(chunk.getChunk());
      chunk = fileReader.readNext();
    }
    try {
      line.put(new byte[]{-1});
      streamComplete = true;
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
  }
  /**
   * Check for line termination bytes or accumulate.
   * @param unicodeBytes
   * @throws IOException
   */
  private void splitBytes(final byte[] unicodeBytes) throws IOException
  {
      byte[] oneSplit;
      int len = unicodeBytes.length;
      int posOffset = 0; 
      int pos;
      for (pos = 0; pos < len; pos++)
      {
          if(pos < len - 1 && unicodeBytes[pos] == CARRIAGE_RETURN && unicodeBytes[pos+1] == LINE_FEED)
          {
            oneSplit = Arrays.copyOfRange(unicodeBytes, posOffset, pos);
            pos++;
            
            posOffset = pos +1;
            accumulate(oneSplit);
            try {
              offer();
            } catch (InterruptedException e) {
              throw new InterruptedIOException();
            }            
          }
          else if(unicodeBytes[pos] == LINE_FEED || unicodeBytes[pos] == CARRIAGE_RETURN)
          {
            oneSplit = Arrays.copyOfRange(unicodeBytes, posOffset, pos);
            
            posOffset = pos +1;
            accumulate(oneSplit);
            try {
              offer();
            } catch (InterruptedException e) {
              throw new InterruptedIOException();
            }
          }
                                          
      }
      
      try 
      {
        oneSplit = Arrays.copyOfRange(unicodeBytes, posOffset, pos);
        accumulate(oneSplit);
      } catch (Exception e) {
        
      }
      
  }
  
  private void offer() throws InterruptedException {
    if (lineBytesAccumulated.length > 0) {
      line.put(lineBytesAccumulated);
    }
    lineBytesAccumulated = null;
  }
  private void accumulate(byte[] oneSplit)
  {
    if (lineBytesAccumulated == null) {
      
      lineBytesAccumulated = oneSplit;
    }
    else
    {
      byte[] tmp = new byte[lineBytesAccumulated.length + oneSplit.length];
      System.arraycopy(lineBytesAccumulated, 0, tmp, 0, lineBytesAccumulated.length);
      System.arraycopy(oneSplit, 0, tmp, lineBytesAccumulated.length, oneSplit.length);
      lineBytesAccumulated = tmp;
    }
  }
  @Override
  public void close() throws IOException {
    fileReader.close();
    if(fetchThread.isAlive())
      fetchThread.interrupt();
  }
  @Override
  public void run() {
    try {
      Class.forName(ByteArrayBuilder.class.getName());
    } catch (Exception e1) {
      log.warning("sun.misc.Unsafe not loaded");
    }
    try {
      fetch();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
  private char[] charBuffer;
  @SuppressWarnings("unused")
  private int read0(char[] cbuff, int off, int len) throws IOException
  {

    if(streamComplete)
      throw new IOException("End of stream");
    if(charBuffer == null)
    {
      byte[] bytes = null;
      try {
        bytes = line.take();
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
      if(bytes == null)
        throw new IOException(new IllegalStateException("Fetched null bytes"));
      if(isEOF(bytes))
        return -1;
      
      charBuffer = new String(bytes, StandardCharsets.UTF_8).toCharArray();
    }
    if(charBuffer.length > len)
    {
      System.arraycopy(charBuffer, 0, cbuff, off, len);
      charBuffer = Arrays.copyOfRange(charBuffer, len, charBuffer.length);
      return len;
    }
    else
    {
      System.arraycopy(charBuffer, 0, cbuff, off, charBuffer.length);
      int c_len = charBuffer.length;
      charBuffer = null;
      return c_len;
    }
  
  }
  @Override
  public int read(char[] cbuff, int off, int len) throws IOException {throw new UnsupportedOperationException("Use MemoryMappedReader::readLine()");}
}

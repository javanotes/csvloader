/* ============================================================================
*
* FILE: SocketReaderTask.java
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
package com.reactivetechnologies.csvloader.net;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.reactivetechnologies.csvloader.io.AsciiFileReader;


/**
 * The task that reads the received byte stream and can 
 * return a {@linkplain Reader}. 
 */
class SocketReaderTask implements Runnable
{
  /**
   * 
   */
  private final SimpleSocketListener socketServer;
  private SynchronousQueue<Integer> commQ;
  private ByteBuffer writeBuffer;
  private BlockingQueue<byte[]> out;
  private final UTF8StreamReader streamReader;
  /**
   * Use {@link UTF8StreamReader#readLine() readLine()} to fetch next line.
   * @see {@linkplain AsciiFileReader#readLine()}
   * @return
   */
  public UTF8StreamReader getSocketReader() {
    return streamReader;
  }
  /**
   * 
   * @param socketLoader
   * @throws IOException
   */
  public SocketReaderTask(SimpleSocketListener socketLoader) throws IOException
  {
    this.socketServer = socketLoader;
    out = new ArrayBlockingQueue<>(32);
    streamReader = new UTF8StreamReader(out);
    writeBuffer = ByteBuffer.allocate(socketServer.getBuffSize()*2);
    commQ = new SynchronousQueue<>();
    
  }
  void offer(int read) throws InterruptedException
  {
    commQ.put(read);
  }
  void stop()
  {
    try {
      offer(-1);
    } catch (InterruptedException e) {
      
    }
  }
  @Override
  public void run() {
    boolean running = true;
    while(running)
    {
      int read;
      try 
      {
        read = commQ.take();
        if(read == -1)
          running = false;
        
        try {
          handleBytes(read);
        } catch (IOException e) {
          SimpleSocketListener.log.log(Level.SEVERE, "While writing received bytes to handler", e);
        }
        
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    try {
      streamReader.close();
    } catch (IOException e) {
      
    }
  }

  private static final Logger log = Logger.getLogger(SocketReaderTask.class.getSimpleName());
  private void handleBytes(int read) throws IOException {
    writeBuffer.clear();
    if (read != -1) {
      socketServer.transferBytes(writeBuffer);
    }
    else
    {
      writeBuffer.put((byte) read);
      log.fine("put EOF");
    }
    
    writeBuffer.flip();
    byte[] bytes = new byte[writeBuffer.limit()];
    writeBuffer.get(bytes);
    try {
      while(!out.offer(bytes, 10, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      log.warning(e.toString());
      Thread.currentThread().interrupt();
    }
    log.fine("written to out buffer");
  }
  
}
/* ============================================================================
*
* FILE: MemoryMappedChunkHandler.java
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;
/**
 * Reads and writes using a {@linkplain FileChannel}
 */
class ByteChannelChunkHandler extends AbstractFileChunkHandler implements Closeable{
  private static final Logger log = Logger.getLogger(ByteChannelChunkHandler.class.getSimpleName());
  private FileChannel fileChannel;
  private boolean direct;
  /**
   * 
   * Write mode.
   * @throws IOException
   */
  public ByteChannelChunkHandler(String writeDir, int buffSize, boolean direct, String fileName) throws IOException
  {
    super(writeDir);
    readSize = buffSize;
    this.direct = direct;
  }
  @Override
  protected void moveExistingFile() throws IOException
  {
    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE, 
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.APPEND);
    
    buffer = direct ? ByteBuffer.allocateDirect(readSize) : ByteBuffer.allocate(readSize);
    
    log.info("Write ready for target file ["+file+"]. Existing file (if any) was truncated.");
  }
  /**
   * Write mode with default buffer size of 8192
   * @param writeDir
   * @throws IOException
   */
  public ByteChannelChunkHandler(String writeDir, String fileName) throws IOException
  {
    this(writeDir, 8192, false, fileName);
  }
  /**
   * Read mode with default chunk size of 8192.
   * @param f
   * @param chunkSize
   * @throws IOException
   */
  public ByteChannelChunkHandler(File f) throws IOException {
    this(f, 8192);
  }
  /**
   * Read mode.
   * @param f
   * @param chunkSize
   * @throws IOException
   */
  public ByteChannelChunkHandler(File f, int chunkSize) throws IOException {
    super(f);
    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    readSize = chunkSize;
    chunks = fileSize % readSize == 0 ? (int) ((fileSize / readSize)) : (int) ((fileSize / readSize) + 1);
    buffer = ByteBuffer.allocate(readSize);
    debugInitialParams();
    log.info("Reading source file of ["+fileSize+"] bytes. Expected chunks to read "+chunks+","
        + " with chunk size "+readSize);
  }
  private ByteBuffer buffer;
  protected int chunks;
  @Override
  public void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.force(true);
      fileChannel.close();
    }
    if(buffer != null)
    {
      buffer.clear();
      MemoryMappedChunkHandler.unmap(buffer);
      buffer = null;
    }
  }

  protected int idx = 0;
  protected int readSize;
  @Override
  public FileChunk readNext() throws IOException {
      
    int read = fileChannel.read(buffer);
    if(read == -1)
      return null;
    buffer.flip();
    byte[] bytes = new byte[read];
    buffer.get(bytes);
    buffer.clear();
    FileChunk chunk = new FileChunk(fileName, fileSize, creationTime, lastAccessTime, lastModifiedTime);
    chunk.setChunk(bytes);
    chunk.setOffset(idx++);
    chunk.setSize(chunks);
    //log.debug("[readNext] "+chunk);
    return chunk;
    
  }
  
  /**
   * 
   * @param chunk
   * @throws IOException
   */
  @Override
  public void writeNext(FileChunk chunk) throws IOException {
    
    if(file == null)
    {
      initWriteFile(chunk);
            
    }
    
    fileSize += chunk.getChunk().length;
    
    buffer.clear();
    buffer.put(chunk.getChunk());
    buffer.flip();
    fileChannel.write(buffer);
        
  }
  

}

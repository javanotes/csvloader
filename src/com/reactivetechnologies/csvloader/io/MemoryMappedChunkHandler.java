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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;

import com.reactivetechnologies.csvloader.ConfigLoader;
/**
 * Reads and writes using mapped byte buffer
 */
class MemoryMappedChunkHandler extends AbstractFileChunkHandler implements Closeable{
  private static final Logger log = Logger.getLogger(MemoryMappedChunkHandler.class.getSimpleName());
  private FileChannel iStream;
  private FileChannel oStream;
  //private static final Logger log = LoggerFactory.getLogger(MemoryMappedChunkHandler.class);
  public static final long DEFAULT_MEM_MAP_SIZE = Long.parseLong(System.getProperty(ConfigLoader.SYS_PROP_MMAP_SIZE, "8388608"));
  /**
   * 
   * Write mode.
   * @throws IOException
   * @deprecated Experimental. Not to be used.
   */
  private MemoryMappedChunkHandler(String writeDir) throws IOException
  {
    super(writeDir);
    log.warning("MemoryMappedChunkHandler opened in write mode, which is not supported.");
  }
  /**
   * Read mode with default chunk size of 8192.
   * @param f
   * @param chunkSize
   * @throws IOException
   */
  public MemoryMappedChunkHandler(File f) throws IOException {
    this(f, 8192);
  }
  /**
   * Read mode.
   * @param f
   * @param chunkSize
   * @throws IOException
   */
  public MemoryMappedChunkHandler(File f, int chunkSize) throws IOException {
    super(f);
    iStream = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    readSize = chunkSize;
    chunks = fileSize % readSize == 0 ? (int) ((fileSize / readSize)) : (int) ((fileSize / readSize) + 1);
    mapSize = DEFAULT_MEM_MAP_SIZE < getFileSize() ? DEFAULT_MEM_MAP_SIZE : getFileSize();
    allocate();
    debugInitialParams();
    log.info("Reading source file of ["+fileSize+"] bytes. Expected chunks to read "+chunks+","
        + " with chunk size "+readSize+" and mapped region size: "+mapSize);
  }
  private long mapSize;
  protected int chunks;
  
  private void allocate() throws IOException
  {
    if(mapBuff != null){
      unmap(mapBuff);
    }
    long remaining = fileSize - position;
    mapBuff = iStream.map(MapMode.READ_ONLY, position, remaining > mapSize ? mapSize : remaining);
  }
  @Override
  public void close() throws IOException {
    if (iStream != null) {
      iStream.force(true);
      iStream.close();
    }
    if(oStream != null){
      oStream.force(true);
      oStream.close();
    }
    if(mapBuff != null)
      unmap(mapBuff);
  }

  protected int idx = 0;
  protected int readSize;
  @Override
  public FileChunk readNext() throws IOException {
        
    if(!mapBuff.hasRemaining())
    {
      if(position == getFileSize())
        return null;
      allocate();
      
    }
    byte[] read = new byte[mapBuff.remaining() > readSize ? readSize : mapBuff.remaining()];
    mapBuff.get(read);
    position += read.length;
    
    FileChunk chunk = new FileChunk(fileName, fileSize, creationTime, lastAccessTime, lastModifiedTime);
    chunk.setChunk(read);
    chunk.setOffset(idx++);
    chunk.setSize(chunks);
    
    return chunk;
    
  }
  protected MappedByteBuffer mapBuff;
  private long position = 0;
  /**
   * 
   * @param chunk
   * @throws IOException
   * @deprecated Experimental. Not to be used.
   */
  @Override
  public void writeNext(FileChunk chunk) throws IOException {
    //log.debug("[writeNext] "+chunk);
    if(file == null)
    {
      initWriteFile(chunk);
      oStream = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
      
      /*
       * From javadocs:
       * "The behavior of this method when the requested region is not completely contained within this channel's file is unspecified. 
       * Whether changes made to the content or size of the underlying file, by this program or another, are propagated to the buffer 
       * is unspecified. The rate at which changes to the buffer are propagated to the file is unspecified."
       * 
       * Initially this is a 0 byte file. So how do we write to a new file??
       */
      //log.debug("mapping byte buffer for write");
      mapBuff = oStream.map(MapMode.READ_WRITE, 0, chunk.getFileSize());
      
      /*if(log.isDebugEnabled())
      {
        debugInitialParams();
        log.debug("Writing to target file- "+file+". Expecting chunks to receive- "+chunk.getSize());
      }*/
    }
    
    
    doAttribCheck(chunk);
    
    
    //this is probably unreachable
    if(!mapBuff.hasRemaining())
    {
      position += mapBuff.position();
      unmap(mapBuff);
      mapBuff = oStream.map(MapMode.READ_WRITE, position, chunk.getFileSize());
    }
    
    mapBuff.put(chunk.getChunk());
    fileSize += chunk.getChunk().length;
    
    if(fileSize > chunk.getFileSize())
      throw new IOException("File size ["+fileSize+"] greater than expected size ["+chunk.getFileSize()+"]");
        
  }
  /**
   *   
   * @param bb
   * @return
   */
  static boolean unmap(ByteBuffer bb)
  {
    /*
     * From  sun.nio.ch.FileChannelImpl
     * private static void  unmap(MappedByteBuffer bb) {
          
           Cleaner cl = ((DirectBuffer)bb).cleaner();
           if (cl != null)
               cl.clean();


       }
     */
    Method cleaner_method = null, clean_method = null;
    try 
    {
      if(cleaner_method == null)
      {
        cleaner_method = findMethod(bb.getClass(), "cleaner");
        cleaner_method.setAccessible(true);
      }
      if (cleaner_method != null) {
        Object cleaner = cleaner_method.invoke(bb);
        if (cleaner != null) {
          if (clean_method == null) {
            clean_method = findMethod(cleaner.getClass(), "clean");
            clean_method.setAccessible(true);
          }
          clean_method.invoke(cleaner);
          return true;
        } 
      }

    } catch (Throwable ex) 
    {
      //ignored   
    }
    return false;
  }
  
  private static Method findMethod(Class<?> class1, String method) throws NoSuchMethodException {
    for(Method m : class1.getDeclaredMethods())
    {
      if(m.getName().equals(method))
        return m;
    }
    throw new NoSuchMethodException(method);
  }

}

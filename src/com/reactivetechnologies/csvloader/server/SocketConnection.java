/* ============================================================================
*
* FILE: SocketConnection.java
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
package com.reactivetechnologies.csvloader.server;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * 
 */
class SocketConnection implements Closeable, Runnable{

  private static final Logger log = Logger.getLogger(SocketConnection.class.getSimpleName());
  private SelectionKey selKey;
 
  private final SocketChannel channel;
  private final ProtocolHandler handler;
  /**
   * 
   * @param r2aBuff
   * @param channel
   * @throws IOException
   */
  SocketConnection(SocketChannel channel, ProtocolHandler handler) throws IOException
  {
    this.channel = channel;
    this.handler = handler;
  }
  
  private DataInputStream inStream;
  
  /**
   * A stateless one time execution of a request/response flow with a remote client.
   * For stateful semantics, a session state should be managed at application level.
   * TODO: This method should be overridden as necessary.
   */
  private void read()
  {
    try 
    {
      boolean complete = handler.doRead(channel);
      if(complete){
        inStream = new DataInputStream(handler.getReadStream());
        synchronized (readComplete) {
          readComplete.getAndSet(complete);
          readComplete.notify();
        }
      }
      
    } catch (IOException e) {
      log.log(Level.SEVERE, "Exception while reading from socket. Cleaning connection", e);
      try {
        close();
      } catch (IOException e1) {
        log.warning(e.getMessage());
      }
    }
    
  }
  private AtomicBoolean readComplete = new AtomicBoolean();
  /**
   * 
   */
  private class ReaderTask implements Runnable
  {

    @Override
    public void run() {
      read();
      
    }
    
  }
  private void runProcess()
  {
    try 
    {
      synchronized (readComplete) {
        while(!readComplete.get())
        {
          try {
            readComplete.wait(1000);
          } catch (Exception e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      byte[] response = handler.doProcess(inStream);
      writeBuff = ByteBuffer.wrap(response);
      selKey.interestOps(SelectionKey.OP_WRITE);
    } catch (Exception e) {
      log.log(Level.SEVERE, "Exception while processing", e);
    }
  }
  private ByteBuffer writeBuff;
    
  /**
   * writer task
   */
  private class WriterTask implements Runnable
  {
    
    public WriterTask() {
      
    }
    @Override
    public void run() {
      try 
      {
        channel.write(writeBuff);
        if(!writeBuff.hasRemaining())
          close();
      } catch (IOException e) {
        log.log(Level.SEVERE, "Exception while writing response", e);
      }
      
    }
    
  }
      
  @Override
  public void close() throws IOException {
    channel.finishConnect();
    channel.close();
    selKey.cancel();
    log.info("Connection closed..");
  }
  /**
   * submit response to processor threads.
   */
  void fireWrite() {
    new WriterTask().run();  
  }
  
  /**
   * 
   */
  void fireRead() {
    new ReaderTask().run();    
  }
  public SelectionKey getSelKey() {
    return selKey;
  }
  public void setSelKey(SelectionKey selKey) {
    this.selKey = selKey;
  }
  @Override
  public void run() {
    runProcess();
    
  }
}

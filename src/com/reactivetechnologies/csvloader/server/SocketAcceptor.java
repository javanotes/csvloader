/* ============================================================================
*
* FILE: SocketAcceptor.java
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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class SocketAcceptor implements Runnable {

  static final Logger log = Logger.getLogger(SocketAcceptor.class.getSimpleName());
  
  // The selector we'll be monitoring
  private final Selector selector;
  private volatile boolean running = false;
  private ExecutorService execThreads;

  protected int threadCounter = 1;
  private BlockingQueue<SocketConnector> queuedConn;
  /**
   * 
   * @param conn
   * @throws IOException
   */
  public void offer(SocketConnector conn) throws IOException
  {
    try {
      while(!queuedConn.offer(conn, 100, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    selector.wakeup();
    
  }
  /**
   * 
   * @param execThreads
   * @throws IOException
   */
  public SocketAcceptor(int maxThread) throws IOException {
    this.selector = Selector.open();
    this.execThreads = Executors.newFixedThreadPool(maxThread, new ThreadFactory() {
      
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Socket.Executor-"+threadCounter ++);
        return t;
      }
    });
    
    queuedConn = new ArrayBlockingQueue<>(1024);
  }
  public void stop()
  {
    running = false;
    selector.wakeup();
    execThreads.shutdown();
    try {
      execThreads.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    log.info("["+Thread.currentThread().getName()+"] Stopped acceptor ..");
  }
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void read(SelectionKey key) throws IOException {
    
    SocketChannel channel = ((SocketChannel) key.channel());
    SocketConnector conn = (SocketConnector) key.attachment();
    
    try 
    {
     
      if (channel.isOpen()) {
        if(conn.fireRead())
        {
          execThreads.submit(conn);
        }
      }
      else
      {
        disconnect(key);
        log.info("["+Thread.currentThread().getName()+"] Remote client disconnected..");
      }
      
    } catch (IOException e) {
      log.log(Level.WARNING, "["+Thread.currentThread().getName()+"] Remote connection force closed", e);
      disconnect(key);
    }
    
  }
  
  private void disconnect(SelectionKey key) throws IOException 
  {
    ((SocketConnector) key.attachment()).close();
    
  }
  
  private void write(SelectionKey key) throws IOException 
  {
    SocketChannel channel = ((SocketChannel) key.channel());
    SocketConnector conn = (SocketConnector) key.attachment();
    try 
    {
     
      if (channel.isOpen()) {
        conn.fireWrite();
      }
      else
      {
        disconnect(key);
        log.info("["+Thread.currentThread().getName()+"] Remote client disconnected..");
      }
      
    } catch (IOException e) {
      log.log(Level.WARNING, "["+Thread.currentThread().getName()+"] Remote connection force closed", e);
      disconnect(key);
    }
    
  }
  
  private void doSelect() throws IOException
  {

    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Set<SelectionKey> keySet = selector.selectedKeys();
    Iterator<SelectionKey> selectedKeys = keySet.iterator();
    
    while (selectedKeys.hasNext()) {
      SelectionKey key = selectedKeys.next();
      
      if (!key.isValid()) {
        selectedKeys.remove();
        disconnect(key);
        continue;
      }
      
      // Check what event is available and deal with it
      try {
        
        if (key.isValid() && key.isReadable()) {
          selectedKeys.remove();
          read(key);
        }
        else if (key.isValid() && key.isWritable()) {
          selectedKeys.remove();
          write(key);
        }
      } catch (IOException e) {
        log.log(Level.WARNING, "["+Thread.currentThread().getName()+"] Ignoring exception and removing connection", e);
        disconnect(key);
      }
    }
  
  }
  
  private void accept(SocketConnector conn) throws IOException
  {
    SocketChannel channel = conn.getChannel();
    conn.setSelKey(channel.register(selector, SelectionKey.OP_READ, conn));
    log.info("["+Thread.currentThread().getName()+"] Accepted connection from remote host "+channel.getRemoteAddress());
  }
  @Override
  public void run() {
    running = true;
    log.info("["+Thread.currentThread().getName()+"] Acceptor started .. ");
    while (running) 
    {
      
      
      SocketConnector conn = queuedConn.poll();
      if(conn != null)
      {
        try {
          accept(conn);
        } catch (IOException e) {
          log.severe("Error while trying to accept new connection => "+e.getMessage());
          try {
            conn.close();
          } catch (IOException e1) {
            
          }
        }
      }
      try 
      {
        doSelect();
      } 
      catch (Exception e) {
        log.log(Level.SEVERE, "["+Thread.currentThread().getName()+"] Unexpected exception in selector loop", e);
      }
    
    }
    

  }

}

/* ============================================================================
*
* FILE: ServerSocketListener.java
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple socket server.
 */
public class ServerSocketListener implements Runnable{

  static
  {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tr %3$s %4$s:  %5$s%6$s %n");
  }
  public static final int DEFAULT_READ_BUFF_SIZE = 64;
  static final Logger log = Logger.getLogger(ServerSocketListener.class.getSimpleName());
  private int port;

  // The channel on which we'll accept connections
  private ServerSocketChannel serverChannel;

  // The selector we'll be monitoring
  private Selector selector;

  private volatile boolean running;
  //private ExecutorService worker;
  private int buffSize;
  private int maxThread;
  private ProtocolHandlerFactory protoFactory = new ProtocolHandlerFactory();
  /**
   * 
   * @param port
   * @param readBufferSize
   * @param maxThread
   * @throws IOException
   */
  public ServerSocketListener(int port, int readBufferSize, int maxThread) throws IOException {
    this.port = port;
    ByteBuffer.allocate(readBufferSize);
    setBuffSize(readBufferSize);
    this.maxThread = maxThread;
    initSelector();
    
  }
  private int threadCounter = 0;
  /**
   * Server running on given port and max thread count based on no of processors.
   * @param port
   * @throws IOException
   */
  public ServerSocketListener(int port) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE, Runtime.getRuntime().availableProcessors());
    
  }
  /**
   * 
   * @param port
   * @param maxThread
   * @throws IOException
   */
  public ServerSocketListener(int port, int maxThread) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE, maxThread);
    
  }

  public static void main(String[] args) {
    try 
    {
      final ServerSocketListener loader = new ServerSocketListener(Integer.valueOf(args[0]));
      loader.startServer();
      Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run()
        {
          loader.stopServer();
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
      log.severe("<Not started> ** "+e.getMessage()+" **");
    }
  }
  private void initSelector() throws IOException {
    // Create a new selector
    selector = Selector.open();

    // Create a new non-blocking server socket channel
    serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);

    // Bind the server socket to the specified address and port
    InetSocketAddress isa = new InetSocketAddress(port);
    try {
      serverChannel.bind(isa);
    } catch (Exception e) {
      log.warning("Unable to start on provided port. Will select auto. Error => "+e.getMessage());
      serverChannel.bind(null);
      isa = (InetSocketAddress) serverChannel.getLocalAddress();
      port = isa.getPort();
    }

    // Register the server socket channel, indicating an interest in
    // accepting new connections
    serverChannel.register(selector, serverChannel.validOps());

    /*ioThreads = Executors.newFixedThreadPool(maxThread/2, new ThreadFactory() {
      
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Socket.IO.Worker-"+threadCounter++);
        return t;
      }
    });*/
    
    execThreads = Executors.newFixedThreadPool(maxThread, new ThreadFactory() {
      
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Socket.Exec.Worker-"+threadCounter++);
        return t;
      }
    });
  }
 
  
  private ExecutorService execThreads;
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void accept(SelectionKey key) throws IOException {

    SocketChannel socketChannel = serverChannel.accept();
    
    socketChannel.configureBlocking(false);
    
    SocketConnection conn = new SocketConnection(socketChannel, protoFactory.isSingleton() ? protoHandlerSingleton : protoFactory.getObject());
    conn.setSelKey(socketChannel.register(selector, SelectionKey.OP_READ, conn));
    execThreads.submit(conn);
    
    log.info("Accepted connection from remote host "+socketChannel.getRemoteAddress());
  }
  
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void read(SelectionKey key) throws IOException {
    
    SocketChannel channel = ((SocketChannel) key.channel());
    SocketConnection conn = (SocketConnection) key.attachment();
    try 
    {
      
      //TODO: move read to acceptor
      if (channel.isOpen()) {
        conn.fireRead();
      }
      else
      {
        removeConnection(key);
        log.info("Remote client disconnected..");
      }
      
    } catch (IOException e) {
      log.log(Level.WARNING, "Remote connection force closed", e);
      removeConnection(key);
    }
    
    log.fine("Channel read complete");
  }
  
  private void doSelect() throws IOException
  {

    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
    
    while (selectedKeys.hasNext()) {
      SelectionKey key = selectedKeys.next();
      selectedKeys.remove();

      if (!key.isValid()) {
        removeConnection(key);
        continue;
      }
      
      // Check what event is available and deal with it
      try {
        if (key.isAcceptable()) {
          accept(key);
        }
        else if (key.isValid() && key.isReadable()) {
          read(key);
        }
        else if (key.isValid() && key.isWritable()) {
          write(key);
        }
      } catch (IOException e) {
        log.log(Level.WARNING, "Ignoring exception and removing connection", e);
        removeConnection(key);
      }
    }
  
  }
  
  private void disconnect(SocketConnection conn) throws IOException
  {
    conn.close();
    
  }
  @Override
  public void run() {
    running = true;
    log.info("Listening on port "+port+" for connection.. ");
    while (running) 
    {

      try 
      {
        doSelect();
      } 
      catch (Exception e) {
        log.log(Level.SEVERE, "Unexpected exception in selector loop", e);
      }
    

    }
    close0();
    log.info("Stopped listening ..");
  }
  private void write(SelectionKey key) throws IOException {
    SocketConnection conn = (SocketConnection) key.attachment();
    conn.fireWrite();
    
  }

  /**
   * Stops the server.
   */
  public void stopServer()
  {
    running = false;
    selector.wakeup();
  }

  
  private void close0() {
    try {
      selector.close();
    } catch (IOException e) {
      
    }
    
    try {
      serverChannel.close();
    } catch (IOException e) {
      
    }
    execThreads.shutdown();
    try {
      execThreads.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
  }
  /**
   * Starts the server
   */
  public void startServer() {
    new Thread(this, "Socket.Acceptor").start();
  }
  public int getBuffSize() {
    return buffSize;
  }
  public void setBuffSize(int buffSize) {
    this.buffSize = buffSize;
  }
  /**
   * 
   * @param channel
   * @param key
   * @throws IOException
   */
  void removeConnection(SelectionKey key) throws IOException {
    disconnect((SocketConnection) key.attachment());
  }
  private ProtocolHandler protoHandlerSingleton;
  /**
   * Registers a protocol factory to handle and process message. Should be invoked before {@link #startServer()}.
   * @param protoFactory
   */
  public void registerProtocolFactory(ProtocolHandlerFactory protoFactory) {
    if(!running)
    {
      this.protoFactory = protoFactory;
      if(protoFactory.isSingleton())
      {
        protoHandlerSingleton = protoFactory.getObject();
      }
    }
  }
}

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
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A lightweight and fast, fixed length bytes protocol server, based on non-blocking NIO. Designed using a reactor pattern with a single thread for IO operations, 
 * and multiple threads for process execution.<p>Extend {@linkplain ProtocolHandlerFactory} to provide a 
 * custom {@linkplain ProtocolHandler} and override {@linkplain ProtocolHandler#doProcess(java.io.DataInputStream) doProcess()} 
 * and {@linkplain ProtocolHandler#doRead(SocketChannel) doRead()} methods. 
 * Register the factory using {@link #registerProtocolFactory(ProtocolHandlerFactory) registerProtocolFactory()} before invoking {@link #startServer()}.
 * <p>
 * 
 * @see ProtocolHandler
 */
public class ServerSocketListener implements Runnable{

  static
  {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tr %3$s %4$s:  %5$s%6$s %n");
  }
  public static final int DEFAULT_READ_BUFF_SIZE = 32;
  static final Logger log = Logger.getLogger(ServerSocketListener.class.getSimpleName());
  private int port;

  // The channel on which we'll accept connections
  private ServerSocketChannel serverChannel;

  private Selector selector;
  private int buffSize;
  private int maxExecutorThreads, maxAcceptorThreads;
  private ProtocolHandlerFactory protoFactory = new ProtocolHandlerFactory();
  
  /**
   * Instantiate a new server. There will be total (maxExecutorThreads + maxAcceptorThreads + 1) threads running.
   * @param port - listening port
   * @param readBufferSize - read buffer size
   * @param maxExecutorThreads - max process executor threads
   * @param maxAcceptorThreads - max connection acceptor threads
   * @throws IOException
   */
  public ServerSocketListener(int port, int readBufferSize, int maxExecutorThreads, int maxAcceptorThreads) throws IOException {
    this.port = port;
    ByteBuffer.allocate(readBufferSize);
    setBuffSize(readBufferSize);
    this.maxExecutorThreads = maxExecutorThreads;
    this.maxAcceptorThreads = maxAcceptorThreads;
    open();
    
  }
  private int threadCounter = 0;
  /**
   * Server running on given port and max thread count based on no of processors.
   * @param port
   * @throws IOException
   */
  public ServerSocketListener(int port) throws IOException {
    this(port, Runtime.getRuntime().availableProcessors());
    
  }
  /**
   * 
   * @param port
   * @param maxThread
   * @throws IOException
   */
  public ServerSocketListener(int port, int maxThread) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE, maxThread, maxThread/2);
    
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
  private void open() throws IOException {
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
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);

    //single IO thread, multiple execution threads.
    //For multi-threading the IO threads, would require
    //a consistent hashed distribution pattern. Since we need to process the IO in order, for the same connection
        
    execThreads = Executors.newFixedThreadPool(maxAcceptorThreads, new ThreadFactory() {
      
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Socket.Acceptor-"+threadCounter++);
        return t;
      }
    });
    
    acceptors = new LinkedList<>();
    for(int i=0; i<maxExecutorThreads/2; i++)
    {
      acceptors.add(new SocketAcceptor(maxExecutorThreads));
    }
  }
 
  private LinkedList<SocketAcceptor> acceptors;
  
  private ExecutorService execThreads;
  private boolean running;
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void accept(SelectionKey key) throws IOException {

    SocketChannel socketChannel = serverChannel.accept();
    socketChannel.configureBlocking(false);
    SocketConnector conn = new SocketConnector(socketChannel, protoFactory.isSingleton() ? protoHandlerSingleton : protoFactory.getObject());
    //round robin dispatch
    SocketAcceptor sa = acceptors.remove();
    try {
      sa.offer(conn);
    } finally {
      acceptors.addLast(sa);
    }
    
    
  }
  
  private void doSelect() throws IOException
  {

    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Set<SelectionKey> keySet = selector.selectedKeys();
    Iterator<SelectionKey> selectedKeys = keySet.iterator();
    
    while (selectedKeys.hasNext()) 
    {
      SelectionKey key = selectedKeys.next();
      
      if (!key.isValid()) {
        selectedKeys.remove();
        continue;
      }
      
      try 
      {
        if (key.isAcceptable()) {
          selectedKeys.remove();
          accept(key);
        }
        
      } catch (IOException e) {
        log.log(Level.WARNING, "["+Thread.currentThread().getName()+"] Ignoring exception and removing connection", e);
        
      }
    }
  
  }
  
  
  @Override
  public void run() 
  {
    running = true;
    log.info("["+Thread.currentThread().getName()+"] Listening on port "+port+" for connection.. ");
    while(running)
    {
      try {
        doSelect();
      } catch (IOException e) {
        log.log(Level.SEVERE, "["+Thread.currentThread().getName()+"] Unexpected exception in selector loop", e);
      }
    }
    stop();
  }
  
  private void stop() {
    try {
      selector.close();
    } catch (IOException e) {
      
    }
    for(SocketAcceptor socka : acceptors)
    {
      socka.stop();
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
   * Stops the server.
   */
  public void stopServer()
  {
    running = false;
    selector.wakeup();
  }

  
  
  /**
   * Starts the server
   */
  public void startServer() 
  {
    for(SocketAcceptor socka : acceptors)
    {
      execThreads.submit(socka);
    }
    new Thread(this, "Server.Listener").start();
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

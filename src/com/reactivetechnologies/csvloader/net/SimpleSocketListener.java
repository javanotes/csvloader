/* ============================================================================
*
* FILE: SimpleSocketListener.java
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.reactivetechnologies.csvloader.io.AsciiFileReader;

/**
 * A naive socket server that accepts a single client connection, and expects UTF8 encoded
 * byte stream.
 */
class SimpleSocketListener implements Runnable{

  public static final int DEFAULT_READ_BUFF_SIZE = 8192;
  static final Logger log = Logger.getLogger(SimpleSocketListener.class.getSimpleName());
  private int port;

  // The channel on which we'll accept connections
  private ServerSocketChannel serverChannel;

  // The selector we'll be monitoring
  private Selector selector;

  // The buffer into which we'll read data when it's available
  private ByteBuffer readBuffer;
  // The buffer which would contain the bytes as utf8 decoded
  private ByteBuffer transferBuffer;
  private volatile boolean running;
  //private ExecutorService worker;
  private int buffSize;
  private CharsetDecoder utf8Decoder;
  private CharsetEncoder utf8Encoder;
  /**
   * 
   * @param port
   * @param readBufferSize
   * @throws IOException
   */
  public SimpleSocketListener(int port, int readBufferSize) throws IOException {
    this.port = port;
    initSelector();
    readBuffer = ByteBuffer.allocate(readBufferSize);
    this.setBuffSize(readBufferSize);
    transferBuffer = ByteBuffer.allocate(getBuffSize()*2);
    
    utf8Decoder = StandardCharsets.UTF_8.newDecoder();
    utf8Decoder.onMalformedInput(CodingErrorAction.REPORT);
    utf8Decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    utf8Decoder.reset();
    
    utf8Encoder = StandardCharsets.UTF_8.newEncoder();
    utf8Encoder.onMalformedInput(CodingErrorAction.REPORT);
    utf8Encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    utf8Encoder.reset();
  }
  /**
   * 
   * @param port
   * @throws IOException
   */
  public SimpleSocketListener(int port) throws IOException {
    this(port, DEFAULT_READ_BUFF_SIZE);
    
  }

  public static void main(String[] args) {
    try 
    {
      final SimpleSocketListener loader = new SimpleSocketListener(Integer.valueOf(args[0]));
      loader.startServer();
      Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run()
        {
          loader.stopServer();
        }
      });
    } catch (Exception e) {
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

  }
  /**
   * @see {@linkplain AsciiFileReader#readLine()}
   * @return
   * @throws IOException 
   */
  public String readLine() throws IOException
  {
    return workerTask.getSocketReader().readLine();
  }
  private SocketChannel socketChannel;
  private SocketReaderTask workerTask;
  private volatile boolean ready;
  /**
   * Waits until socket is ready to serve.
   * @param timeout
   * @param unit
   * @return
   * @throws InterruptedException
   */
  public boolean isReady(long timeout, TimeUnit unit) throws InterruptedException
  {
    if(!ready)
    {
      synchronized (this) {
        if(!ready)
          wait(unit.toMillis(timeout));
      }
    }
    return ready;
  }
  public boolean isReady() 
  {
    return ready;
  }
  private AtomicInteger clients = new AtomicInteger();
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void accept(SelectionKey key) throws IOException {

    // Accept the connection and make it non-blocking
    if(clients.getAndIncrement() > 0){
      log.warning("* Rejecting more than 1 client request *");
      key.interestOps(0);
      return;
    }
    socketChannel = serverChannel.accept();
    log.info("Accepted connection from remote host "+socketChannel.getRemoteAddress());
    socketChannel.configureBlocking(false);
    
    synchronized (this) {
      workerTask = new SocketReaderTask(this);
      new Thread(workerTask, "Socket.Reader.Worker").start();
      ready = true;
      notifyAll();
    }
    // Register the new SocketChannel with our Selector, indicating
    // we'd like to be notified when there's data waiting to be read
    socketChannel.register(selector, SelectionKey.OP_READ);
  }
  /**
   * 
   * @param read
   * @return
   * @throws CharacterCodingException
   */
  private ByteBuffer convertToUtf8Bytes(ByteBuffer read) throws CharacterCodingException
  {
    try 
    {
      return utf8Encoder.encode(utf8Decoder.decode(read));
    } 
    finally
    {
      utf8Decoder.reset();
      utf8Encoder.reset();
    }
  }
  //2.
  private int convertBytes() throws CharacterCodingException
  {
    synchronized (this) {
      transferBuffer.clear();
      transferBuffer.put(convertToUtf8Bytes(readBuffer));
      //transferBuffer.put(readBuffer);
      transferBuffer.flip();
      return transferBuffer.limit();
    }
  }
  //3.
  void transferBytes(final ByteBuffer destn)
  {
    synchronized (this) {
      destn.put(transferBuffer);
      
    }
  }
  
  /**
   * 
   * @param key
   * @throws IOException
   */
  private void read(SelectionKey key) throws IOException {
    
    // Attempt to read off the channel
    int numRead;
    int totalByteRead = 0;
    try 
    {
      //1.
      synchronized (this) 
      {
        readBuffer.clear();
        do 
        {
          numRead = socketChannel.read(readBuffer);
          if(numRead != -1)
            totalByteRead += numRead;
          
        } while (numRead != -1 && readBuffer.hasRemaining());
        
        readBuffer.flip();
      }
      if(numRead != -1)
      {
        try 
        {
          try {
            totalByteRead = convertBytes();
          } catch (CharacterCodingException e) {
            throw new IOException("Not UTF8 encoded byte stream", e);
          }
          workerTask.offer(totalByteRead);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      else
      {
        try {
          if (totalByteRead > 0) {
            try {
              totalByteRead = convertBytes();
            } catch (CharacterCodingException e) {
              throw new IOException("Not UTF8 encoded byte stream", e);
            }
            workerTask.offer(totalByteRead);
          }
        } catch (InterruptedException e) {
          
        }
        disconnect(key);
        log.info("Remote client disconnected..");
      }
    } catch (IOException e) {
      log.log(Level.WARNING, "Remote connection force closed", e);
      disconnect(key);
    }
    
  }
  
  private void disconnect(SelectionKey key) throws IOException
  {
    workerTask.stop();
    socketChannel.close();
    key.cancel();
  }
  
  private void doSelect() throws IOException
  {
    // Wait for an event one of the registered channels
    selector.select();

    // Iterate over the set of keys for which events are available
    Iterator<SelectionKey> selectedKeys = selector.selectedKeys()
        .iterator();
    while (selectedKeys.hasNext()) {
      SelectionKey key = selectedKeys.next();
      selectedKeys.remove();

      if (!key.isValid()) {
        continue;
      }
      
      int readyOps = key.readyOps();
      // Disable the interest for the operation
      // that is ready. This prevents the same 
      // event from being raised multiple times.
      key.interestOps(key.interestOps() & ~readyOps);

      // Check what event is available and deal with it
      if (key.isAcceptable()) {
        accept(key);
      }
      else if (key.isReadable()) {
        read(key);
      }
    }
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
        log.log(Level.SEVERE, "", e);
      }
    

    }
    close0();
    log.info("Stopped listening ..");
  }
  /**
   * 
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
  }
  /**
   * 
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
}

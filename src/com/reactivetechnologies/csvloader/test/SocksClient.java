/* ============================================================================
*
* FILE: SocksClient.java
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
package com.reactivetechnologies.csvloader.test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class SocksClient implements Closeable{

  private final SocketChannel channel;
  private ByteBuffer buff;
  /**
   * 
   * @param host
   * @param port
   * @throws IOException
   */
  public SocksClient(String host, int port) throws IOException
  {
    channel = SocketChannel.open(new InetSocketAddress(host, port));
  }
  /**
   * 
   * @param source
   * @param buffLen
   * @throws IOException
   */
  public void send(ReadableByteChannel source, int buffLen) throws IOException
  {
    buff = ByteBuffer.allocate(buffLen);
    int available = source.read(buff);
    OutputStreamWriter writer = new OutputStreamWriter(channel.socket().getOutputStream(), StandardCharsets.UTF_8);
    CharBuffer charBuf = CharBuffer.allocate(buffLen);
    while(available != -1)
    {
      buff.flip();
      /*charBuf.clear();
      charBuf.put(StandardCharsets.UTF_8.decode(buff));
      charBuf.flip();
      writer.write(Arrays.copyOfRange(charBuf.array(), 0, charBuf.limit()));*/
      channel.write(buff);
      buff.clear();
      available = source.read(buff);
    }
    writer.close();
  }
  
  void sendFile(String fileName) throws IOException
  {
    ReadableByteChannel chn = Channels.newChannel(new FileInputStream(fileName));
    try {
      send(chn, 1024);
    } catch (Exception e) {
      throw e;
    }
    finally
    {
      try {
        chn.close();
      } catch (Exception e) {} 
    }
    
  }
  
  String requestResponseInString(String msg) throws IOException
  {
    OutputStreamWriter writer = new OutputStreamWriter(channel.socket().getOutputStream(), StandardCharsets.UTF_8);
    BufferedReader reader = new BufferedReader(new InputStreamReader(channel.socket().getInputStream(), StandardCharsets.UTF_8));
    try{
      writer.write(msg);
      writer.flush();
      
      String res = reader.readLine();
      return res;
    }
    finally
    {
      try {
        writer.close();
      } catch (Exception e) {
        
      }
      try {
        reader.close();
      } catch (Exception e) {
        
      }
    }
  }
  
  void requestResponseInString() throws IOException
  {
    OutputStreamWriter writer = new OutputStreamWriter(channel.socket().getOutputStream(), StandardCharsets.UTF_8);
    BufferedReader reader = new BufferedReader(new InputStreamReader(channel.socket().getInputStream(), StandardCharsets.UTF_8));
    try{
      writer.write("HELLO");
      writer.flush();
      
      String res = reader.readLine();
      System.out.println("got================== "+res);
    }
    finally
    {
      try {
        writer.close();
      } catch (Exception e) {
        
      }
      try {
        reader.close();
      } catch (Exception e) {
        
      }
    }
  }
  
  void requestResponseInBytes(int i, double d, String s) throws IOException
  {
    DataOutputStream writer = new DataOutputStream(channel.socket().getOutputStream());
    try
    {
      writer.writeInt(i);
      writer.writeDouble(d);
      writer.writeUTF(s);
      writer.flush();
      
    }
    finally
    {
      try {
        
        writer.close();
      } catch (Exception e) {
        
      }
      
    }
  }
  
  static void loadTest(int iteration, String host, int port)
  {
    System.out.println("Starting load run..");
    SocksClient [] clients = new SocksClient[iteration];
    for(int i = 0; i<iteration; i++)
    {
      try {
        clients[i] = new SocksClient(host, port);
      } catch (IOException e) {
        System.err.println("connect failed ["+i+"] => "+e.getMessage());
      }
    }
    for(int i = 0; i<iteration; i++)
    {
      try {
        SocksClient c = clients[i];
        if(c != null)
        {
          String msg = "Sending: "+i;
          //System.out.println("SENT: "+msg+"\tRECV: "+c.requestResponseInString(msg));
          c.requestResponseInBytes(i, i+0.5, msg);
        }
      } catch (IOException e) {
        System.err.println("req/res failed ["+i+"] => "+e.getMessage());
      }
    }
  }
  public static void main(String[] args) {
    /*try 
    {
      SocksClient client = new SocksClient(args[0], Integer.parseInt(args[1]));
      client.requestResponseInBytes(5, 55.5, "hello_world");
      client.close();
    } catch (Exception e) {
      System.err.println("Usage: "+SocksClient.class.getSimpleName()+" <host> <port> <file>");
      e.printStackTrace();
    }*/
    
    loadTest(1000, "localhost", 8192);
    System.out.println("-- End run --");
  }
  @Override
  public void close() throws IOException {
    if (buff != null) {
      buff.clear();
    }
    channel.close();
  }

}

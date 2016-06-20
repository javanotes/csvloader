/* ============================================================================
*
* FILE: MyProtocolHandler.java
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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import com.reactivetechnologies.csvloader.server.AbstractProtocolHandler;

public class MyProtocolHandler extends AbstractProtocolHandler {

  public MyProtocolHandler() {
    super();
  }

  
  private int len = -1;
  
  @Override
  public byte[] doProcess(DataInputStream dataInputStream) throws Exception {
    int arrayLen, reqNo;
    
    System.out.println("################## BEGIN ##################");
    dataInputStream.readInt();//ignore the length param
    reqNo = dataInputStream.readInt();
    System.out.println("reqNo => "+reqNo);
    System.out.println("Double => "+dataInputStream.readDouble());
    arrayLen = dataInputStream.readInt();
    System.out.println("String.Len => "+arrayLen);
    if(arrayLen != -1)
    {
      StringBuilder s = new StringBuilder();
      for(int i=0; i<arrayLen;i++)
      {
        s.append(dataInputStream.readChar());
      }
      
      System.out.println("String => "+s.toString());
    }
    
    System.out.println("################# END ##################");
    return ("SUCCESS."+reqNo).getBytes(StandardCharsets.UTF_8);
  }
  //A protocol handler expecting an int mentioning total size, then int, a double, then an int
  //mentioning the size of an array, and finally the string as char array.
  // so a data '5|2.5|hello' will have a total length of (4 + 8 + 4 + (2*5)) = 26 bytes.
  // in formatted, the first 4 bytes will hold the integer 26.
  @Override
  public boolean doRead(SocketChannel channel) throws IOException
  {
    readBuffer.clear();
    read = channel.read(readBuffer);
            
    if (read > 0) 
    {
      totalRead += read;
      
      if(len == -1 && totalRead >= 4)
      {
        len = ByteBuffer.wrap(readBuffer.array(), 0, 4).asIntBuffer().get()+4;
        
      }
      
      readBuffer.flip();
      writeStream.write(readBuffer.array(), 0, read);
      
      return totalRead == len;
    }
    
    return false;
                
  }


}

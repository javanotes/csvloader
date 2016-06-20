/* ============================================================================
*
* FILE: SocketCSVLoader.java
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.reactivetechnologies.csvloader.CSVLoader;

public class SocketCSVLoader extends CSVLoader {

  private static final Logger log = Logger.getLogger(SocketCSVLoader.class.getSimpleName());
  private SimpleSocketListener socks;
  private String separator;
  private int linesToIgnore;
  /**
   * 
   * @param loadPerThread
   * @param port
   * @param sep
   * @throws IOException
   */
  public SocketCSVLoader(int loadPerThread, int port, String sep, int linesToIgnore) throws IOException {
    super(loadPerThread);
    socks = new SimpleSocketListener(port);
    this.separator = sep;
    socks.startServer();
    this.linesToIgnore = linesToIgnore;
  }
  
  @Override
  public void clean() throws Exception {
    socks.stopServer();
    super.clean();
  }

  private void loadNextLine(String line)
  {
    log.fine("read next line.. "+line);
    try {
      loadNextLine(line, String.valueOf(separator));
    } catch (Exception e) {
      log.log(Level.WARNING, "Line loading error, on line ["+line+"]", e);
    }
  }
  private void load0() throws IOException
  {
    if(!socks.isReady())
      throw new IOException("Socket not ready. Aborting");
    String line = socks.readLine();
    while(line != null)
    {
      if (linesToIgnore == 1) {
        linesToIgnore = 0;
      }
      else
        loadNextLine(line);
      
      line = socks.readLine();
    }
    allocate();
  }
  @Override
  public void load() {
    try {
      while(!socks.isReady(100, TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      log.warning("InterruptedException while waiting for connection to ready");
    }
    startTime = System.currentTimeMillis();
    try {
      load0();
    } catch (IOException e) {
      log.log(Level.SEVERE, "Stream reading error", e);
    }
    
    log.info("End load run");
  }

}

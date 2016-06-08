/* ============================================================================
*
* FILE: TestLoad.java
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

import java.util.Arrays;

import com.reactivetechnologies.csvloader.CSVLoader;

public class TestLoad {

  /*
   * 
    CREATE TABLE discounts (
    id INT NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    expired_date DATE NOT NULL,
    amount DECIMAL(10 , 2 ) NULL,
    PRIMARY KEY (id)
    );
    
    Wed May 21 00:00:00 EDT 2008
   */
  public static void main(String[] args) {
    try 
    {
    CSVLoader.run();
    
  } catch (Exception e) {
    e.printStackTrace();
  }
    /*System.out.println(Arrays.toString(",hello,,got,".split(","))+" "+",hello,,got,".split(",",-1).length);
    System.out.println(Arrays.toString("got".split(","))+" "+"got".split(",",-1).length);
    System.out.println(Arrays.toString("got,".split(","))+" "+"got,".split(",",-1).length);
    System.out.println(Arrays.toString(",got".split(","))+" "+",got".split(",",-1).length);
    System.out.println(Arrays.toString(" , , ".split(","))+" "+",,".split(",",-1).length);
    Integer.valueOf("");
*/  }

}

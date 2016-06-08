/* ============================================================================
*
* FILE: ByteArrayBuilder.java
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
import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class ByteArrayBuilder implements Closeable
{

  public static final String UNSAFE_NOT_FOUND = "UNSAFE_NOT_FOUND";
  private static Unsafe UNSAFE;
  private static final boolean unsafeLoaded;
  private static void loadUnsafe() throws Exception
  {
    try {

      Field f = Unsafe.class.getDeclaredField("theUnsafe");

      f.setAccessible(true);

      UNSAFE = (Unsafe) f.get(null);

    } catch (Exception e) {

      throw e;

    }
  }

  static
  {
    try {
      loadUnsafe();
      unsafeLoaded = true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private long _address = -1;

  private int byteSize = 0;
  /**
   * Get as array of bytes.
   * @return appended bytes
   */
  public byte[] toArray() {

    byte[] values = new byte[byteSize];

    UNSAFE.copyMemory(null, _address,

        values, Unsafe.ARRAY_BYTE_BASE_OFFSET,

        byteSize);

    return values;

  }
  /**
   * New builder instance
   */
  public ByteArrayBuilder()

  {
    this(new byte[0]);

  }
  /**
   * New builder with given bytes
   * @param bytes
   */
  public ByteArrayBuilder(byte[] bytes)

  {
    if(!unsafeLoaded)
      throw new UnsupportedOperationException(UNSAFE_NOT_FOUND);
    append0(bytes);

  }
  /**
   * Append next bytes.
   * @param _bytes
   * @return this builder
   */
  public ByteArrayBuilder append(byte[] _bytes)

  {

    return append1(_bytes);

  }

  private ByteArrayBuilder append0(byte[] _bytes)

  {

    long _address2 = UNSAFE.allocateMemory(byteSize + _bytes.length);

    UNSAFE.copyMemory(_bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, _address2,
        _bytes.length);

    _address = _address2;

    byteSize += _bytes.length;

    return this;

  }

  private ByteArrayBuilder append1(byte[] _bytes)

  {

    long _address2 = UNSAFE.allocateMemory(byteSize + _bytes.length);

    UNSAFE.copyMemory(_address, _address2, byteSize);

    UNSAFE.copyMemory(_bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + byteSize, null,
        _address2, _bytes.length);

    UNSAFE.freeMemory(_address);

    _address = _address2;

    byteSize += _bytes.length;

    return this;

  }

  private void free() {

    UNSAFE.freeMemory(_address);

  }

  @Override
  public void close() {
    if (_address != -1) {
      free();
    }
    
  }

}
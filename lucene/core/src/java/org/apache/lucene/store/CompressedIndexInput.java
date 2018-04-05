/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.InputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Extension of IndexInput, operating over a compressed file.
 * The length and position data is reported over the file as if it were uncompressed.
 * The length of the original data is stored as a footer which is read as the index is created.
 */
public class CompressedIndexInput extends BufferedIndexInput {

  private final IndexInput input;
  private final InputStream decompressed;
  private final long length;

  /** resourceDescription should be a non-null, opaque string
   *  describing this resource; it's returned from
   *  {@link #toString}. */
  protected CompressedIndexInput(IndexInput input) {
    super("CompressedIndexInput(" + input.toString() + ")");
    this.input = input;

    try {
      this.decompressed = new GZIPInputStream(new InputStreamAdapter(input));
      this.length = readLength(input);
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static long readLength(IndexInput input) throws IOException {
    long position = input.getFilePointer();
    try {
      input.seek(input.length() - 8);
      return input.readLong();
    }
    finally {
      input.seek(position);
    }
  }

  /** The number of bytes in the file. */
  public long length() {
    return length;
  }

  /** Expert: implements buffer refill.  Reads bytes from the current position
   * in the input.
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param length the number of bytes to read
   */
  protected void readInternal(byte[] b, int offset, int length)
          throws IOException {
    int read = decompressed.read(b, offset, length);
    if (read != length) {
      // This MUST read length bytes into the target buffer
      throw new IOException("Could not read " + length + " bytes, only managed to read " + read + " bytes");
    }
  }

  /** Expert: implements seek.  Sets current position in this file, where the
   * next {@link #readInternal(byte[],int,int)} will occur.
   * @see #readInternal(byte[],int,int)
   */
  protected void seekInternal(long pos) throws IOException {}

  public void close() throws IOException {
    input.close();
  }

  private static class InputStreamAdapter extends InputStream {

    private final IndexInput input;

    private InputStreamAdapter(IndexInput input) {
      this.input = input;
    }

    @Override
    public int read() throws IOException {
      return input.readByte();
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
      input.readBytes(b, off, len);
      return len;
    }

    @Override
    public long skip(long n) throws IOException {
      input.skipBytes(n);
      return n;
    }

  }

}

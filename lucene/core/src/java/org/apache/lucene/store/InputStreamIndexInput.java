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


import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * A {@link ChecksumIndexInput} wrapping a plain {@link InputStream}.
 */
public class InputStreamIndexInput extends ChecksumIndexInput implements Closeable {
  private final InputStream input;
  private final CRC32 crc;

  public InputStreamIndexInput(String resourceDescription, InputStream input) {
    super(resourceDescription);
    this.crc = new CRC32();
    this.input = new BufferedInputStream(new CheckedInputStream(input, crc));
  }

  @Override
  public byte readByte() throws IOException {
    int v = input.read();
    if (v == -1) throw new EOFException();
    return (byte) v;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      final int cnt = input.read(b, offset, len);
      if (cnt < 0) {
          // Partially read the input, but no more data available in the stream.
          throw new EOFException();
      }
      len -= cnt;
      offset += cnt;
    }
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public final long getChecksum() throws IOException {
    return crc.getValue();
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    throw new UnsupportedOperationException();
  }

}

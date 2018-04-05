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

import java.io.OutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Extension of IndexOutput, operating over a compressed file.
 * The position data is reported over the file as if it were uncompressed.
 * The length of the original data is stored as a footer which is written as the index is closed.
 */
public class CompressedIndexOutput extends OutputStreamIndexOutput {

  /** Default buffer size set to {@value #BUFFER_SIZE}. */
  public static final int BUFFER_SIZE = 1024;

  private final IndexOutput output;

  /** resourceDescription should be a non-null, opaque string
   *  describing this resource; it's returned from
   *  {@link #toString}. */
  public CompressedIndexOutput(IndexOutput output) {
    super("CompressedIndexOutput(" + output.toString() + ")", output.getName(), silentlyWrapStream(output), BUFFER_SIZE);
    this.output = output;
  }

  private static OutputStream silentlyWrapStream(IndexOutput output) {
    try {
      return new GZIPOutputStream(new OutputStreamAdapter(output));
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    output.writeLong(getFilePointer());

    try {
      super.close();
    } finally {
      output.close();
    }
  }

  private static class OutputStreamAdapter extends OutputStream {

    private final IndexOutput output;

    private OutputStreamAdapter(IndexOutput output) {
      this.output = output;
    }

    @Override
    public void write(int b) throws IOException {
      output.writeByte((byte)(b & 0xFF));
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      output.writeBytes(b, off, len);
    }

  }

}

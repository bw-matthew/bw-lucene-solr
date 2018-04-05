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
package org.apache.solr.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.CompressedIndexInput;
import org.apache.lucene.store.CompressedIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;

public class CompressedHdfsDirectory extends HdfsDirectory {

  public CompressedHdfsDirectory(Path hdfsDirPath, Configuration configuration) throws IOException {
    this(hdfsDirPath, HdfsLockFactory.INSTANCE, configuration, DEFAULT_BUFFER_SIZE);
  }

  public CompressedHdfsDirectory(Path hdfsDirPath, LockFactory lockFactory, Configuration configuration, int bufferSize)
      throws IOException {
    super(hdfsDirPath, lockFactory, configuration, bufferSize);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new CompressedIndexOutput(super.createOutput(name, context));
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    return new CompressedIndexOutput(super.createTempOutput(prefix, suffix, context));
  }

  @Override
  public IndexInput openInput(String name, IOContext context)
      throws IOException {
    return new CompressedIndexInput(super.openInput(name, context));
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file stored in a DFS.
 */
public final class DFSManagedBuffer extends ManagedBuffer {
  private final FileSystem hadoopFS;
  private final Path path;
  private final long offset;
  private final long length;

  public DFSManagedBuffer(Path path, long offset, long length)
    throws IOException {

    // TODO Avoid creating Conf
    this.hadoopFS = FileSystem.get(new Configuration());
    this.path = path;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long size() {
    return length;
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream createInputStream() throws IOException {
    FSDataInputStream is = null;
    try {
      is = hadoopFS.open(path);
      ByteStreams.skipFully(is, offset);
      return new LimitedInputStream(is, length);
    } catch (IOException e) {
      try {
        if (is != null) {
          throw new IOException("Error in reading " + this, e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(is);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(is);
      throw e;
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("path", path)
      .add("offset", offset)
      .add("length", length)
      .toString();
  }
}

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

package org.apache.spark.storage

import java.io.File
import java.io.InputStream
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.spark.SparkEnv
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.Serializer
import com.google.common.io.ByteStreams
import java.io.BufferedInputStream
import java.io.FileInputStream

/**
 * Interface to local file system operations for a Spark application.
 */
private[spark] class LocalFileSystem extends FileSystem {
  private lazy val blockManager = SparkEnv.get.blockManager
  private lazy val diskBlockManager = blockManager.diskBlockManager

  // TODO Can we avoid creating this since we just need it for IO streams
  // If not, should get this using FileSystem.get.
  private val localFS = new RawLocalFileSystem
  localFS.initialize(URI.create("file:///"), new Configuration())

  override def open(uri: URI) : FSDataInputStream = {
    localFS.open(new Path(uri))
  }

  override def create(uri: URI, append: Boolean) : FSDataOutputStream = {
    if (append && exists(uri)) {
      localFS.append(new Path(uri))
    } else {
      localFS.create(new Path(uri), true)
    }
  }

  override def getBoundedStream(
      fileStream: FSDataInputStream,
      start: Long,
      end: Long): FSDataInputStream = {

    new FSDataInputStream(
        new BufferedInputStream(ByteStreams.limit(fileStream, end - start)))
  }

  override def truncateStream(fileStream: FSDataInputStream, position: Long) = {
    fileStream.getWrappedStream.asInstanceOf[FileInputStream].getChannel.truncate(position)
  }

  override def wrapForCompression(blockId: BlockId, fileStream: FSDataInputStream): InputStream = {
    blockManager.wrapForCompression(blockId, fileStream)
  }

  override def deleteFile(uri: URI) {
    val file = new File(uri);
    if (file.exists()) {
      file.delete()
    }
  }

  override def getTempFilePath(fileName: String): URI = {
    diskBlockManager.getFile(fileName).toURI()
  }

  override def exists(uri: URI): Boolean = {
    new File(uri).exists()
  }

  override def getFileSize(uri: URI): Long = {
    new File(uri).length
  }

  override def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter = {

    blockManager.getDiskWriter(blockId, file, serializer, bufferSize,
      writeMetrics, this)
  }

  override def getValuesFromDiskStore(
    writer: BlockObjectWriter,
    serializer: Serializer): Option[Iterator[Any]] = {

    blockManager.diskStore.getValues(writer.blockId, serializer)
  }

  override def getShuffleClient(): ShuffleClient = {
    SparkEnv.get.blockManager.shuffleClient
  }

  override def createManagedBuffer(
    file: URI,
    offset: Long,
    length: Long,
    transportConf: TransportConf): ManagedBuffer = {

    new FileSegmentManagedBuffer(
      transportConf,
      new File(file),
      offset,
      length)
  }
}

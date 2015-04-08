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

import java.net.URI
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.Serializer
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.InputStream
import org.apache.spark.SparkEnv
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.BufferedInputStream
import com.google.common.io.ByteStreams
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.network.buffer.DFSManagedBuffer
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.network.shuffle.DFSShuffleClient
import org.apache.spark.network.util.TransportConf
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import org.apache.spark.network.buffer.DFSManagedBuffer
import java.nio.ByteBuffer
import org.apache.spark.util.ByteBufferInputStream
import org.apache.commons.io.input.BoundedInputStream
import java.io.ByteArrayInputStream
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
import org.apache.hadoop.fs.AbstractFileSystem
import org.apache.hadoop.fs.CreateFlag
import java.util.EnumSet
import org.apache.hadoop.fs.Options

/**
 * Interface to HDFS operations for a Spark application.
 */
private[spark] class DistributedFileSystem(
    conf: SparkConf,
    hadoopConf: Configuration)
  extends FileSystem {

  private lazy val blockManager = SparkEnv.get.blockManager
  private val hadoopFS = org.apache.hadoop.fs.FileContext.getFileContext(hadoopConf)
  private lazy val shuffleClient = getDFSShuffleClient
  
  /**
   * Application directory reserved for the node on DFS.
   */
  private lazy val nodeAppDirOnDFS = getNodeAppDirOnDFS

  private def getLocalDirOnDFS: String = {
    hadoopConf.get("fs.defaultFS") + conf.get("spark.dfs.local.dir", "/tmp")
  }

  private def getNodeAppDirOnDFS: URI = {
    val dir = getLocalDirOnDFS +
      "/" + Utils.localHostName() +
      "/spark/" +
      conf.getAppId

    new URI(dir)
  }

  private def getDFSShuffleClient: ShuffleClient = {
    val maxThreads = conf.getInt("spark.shuffle.dfs_client.threads", 1)
    val shuffleClient = new DFSShuffleClient(getLocalDirOnDFS, hadoopConf,
      maxThreads)
    shuffleClient.init(conf.getAppId)

    shuffleClient
  }

  def open(uri: URI) : FSDataInputStream = {
    hadoopFS.open(new Path(uri))
  }

  override def create(uri: URI, append: Boolean) : FSDataOutputStream = {
    val createFlags =
      if (append) {
        EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
      } else {
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
      }

    hadoopFS.create(new Path(uri), createFlags, Options.CreateOpts.createParent)
  }

  override def getBoundedStream(
      fileStream: FSDataInputStream,
      size: Long): FSDataInputStream = {

    new FSDataInputStream(new BoundedInputStream(fileStream, size))
  }

  override def truncate(file: URI, length: Long) = {
    // TODO HDFS-3107 adds support for truncation and is available in 2.7.0.
    // Use reflection based lookup to use the API and maintain backward compatibility.
    if (length == 0) {
      // Just overwrite the file
      create(file, false)
    } else {
      // Copy the contents of the file up to the specified length to a new temp file
      // and finally rename it.
      val truncatedFileName = file.toString + ".truncated"
      val truncatedFile = new URI(truncatedFileName)
      val outputStream = create(file, false)
      try {
        // TODO Handle large file
        getBytes(file, length).map(bytes => outputStream.write(bytes, 0,
          length.toInt))
      } finally {
        outputStream.close
      }

      hadoopFS.rename(new Path(file), new Path(truncatedFile), Rename.OVERWRITE)
    }
  }

  override def wrapForCompression(blockId: BlockId, fileStream: FSDataInputStream): InputStream = {
    // No compression
    fileStream
  }

  override def deleteFile(uri: URI) {
    hadoopFS.delete(new Path(uri), true)
  }

  override def getTempFilePath(fileName: String): URI = {
    new URI(nodeAppDirOnDFS + "/" + fileName)
  }

  override def exists(uri: URI): Boolean = {
    hadoopFS.util.exists(new Path(uri))
  }

  override def getFileSize(uri: URI): Long = {
    hadoopFS.getFileStatus(new Path(uri)).getLen
  }

  override def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter = {

    blockManager.getDiskWriter(blockId, file, serializer, bufferSize, writeMetrics, this)
  }

  override def getValuesFromDiskStore(
    writer: BlockObjectWriter,
    serializer: Serializer): Option[Iterator[Any]] = {

    val file = getTempFilePath(writer.blockId.name)
    val length = getFileSize(file)
    getBytes(file, length).map(bytes => dataDeserialize(writer.blockId, bytes, serializer))
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   *
   * Copied from DiskManager.scala
   */
  private def dataDeserialize(
      blockId: BlockId,
      bytes: Array[Byte],
      serializer: Serializer): Iterator[Any] = {

    val stream = new ByteArrayInputStream(bytes)
    serializer.newInstance().deserializeStream(stream).asIterator
  }

  private def getBytes(file: URI, length: Long): Option[Array[Byte]] = {
    val inputStream = open(file)

    try {
      // TODO Handle large file
      val buf = new Array[Byte](length.toInt)
      inputStream.readFully(0, buf)
      Some(buf)
    } finally {
      inputStream.close
    }
  }

  override def getShuffleClient(): ShuffleClient = {
    shuffleClient
  }

  override def createManagedBuffer(
    file: URI,
    offset: Long,
    length: Long,
    transportConf: TransportConf): ManagedBuffer = {

    new DFSManagedBuffer(new Path(file), offset, length, hadoopConf)
  }

  override def close = {
    if (shuffleClient != null) {
      shuffleClient.close
    }
  }
}

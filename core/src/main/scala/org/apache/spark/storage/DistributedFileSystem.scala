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

/**
 * Interface to HDFS operations for a Spark application.
 */
private[spark] class DistributedFileSystem(
    conf: SparkConf,
    hadoopConf: Configuration)
  extends FileSystem {

  private lazy val blockManager = SparkEnv.get.blockManager
  private val hadoopFS = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  /**
   * Application directory reserved for the node on DFS.
   */
  private lazy val nodeAppDirOnDFS = getNodeAppDirOnDFS

  private def getNodeAppDirOnDFS: URI = {
    val dir = conf.get("spark.dfs.local.dir", "/tmp") +
      "/" + Utils.localHostName() +
      "/spark/" +
      conf.getAppId

    val scheme = hadoopConf.get("fs.defaultFS")
    new URI(scheme + dir)
  }

  def open(uri: URI) : FSDataInputStream = {
    hadoopFS.open(new Path(uri))
  }

  override def create(uri: URI, append: Boolean) : FSDataOutputStream = {
    if (append && exists(uri)) {
      hadoopFS.append(new Path(uri))
    } else {
      hadoopFS.create(new Path(uri), true)
    }
  }

  override def getBoundedStream(
      fileStream: FSDataInputStream,
      start: Long,
      end: Long): FSDataInputStream = {

    // TODO
    fileStream
  }

  override def truncateStream(fileStream: FSDataInputStream, position: Long) = {
    // TODO
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
    hadoopFS.exists(new Path(uri))
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

    // TODO
    throw new RuntimeException("Unsupported")
  }

  override def getShuffleClient(): ShuffleClient = {
    new DFSShuffleClient(hadoopConf)
  }

  override def createManagedBuffer(
    file: URI,
    offset: Long,
    length: Long,
    transportConf: TransportConf): ManagedBuffer = {

    new DFSManagedBuffer(new Path(file), offset, length)
  }
}

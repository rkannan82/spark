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

import java.io.InputStream
import java.net.URI

import scala.collection.Iterator

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.Serializer

/**
 * Interface to file system operations.
 */
private[spark] trait FileSystem {
  /**
   * Returns a handle to read from a file.
   *
   * @throws FileNotFoundException if the file is not present or cannot be opened for read
   */
  def open(uri: URI) : FSDataInputStream

  /**
   * Returns a handle to write to a file.
   *
   * @param uri file
   * @param append if the writes should be appended to existing content.
   *        If false, and the file exists, it will be overwritten.
   */
  def create(uri: URI, append: Boolean) : FSDataOutputStream

  /**
   * Returns an input stream that is bounded by the given byte range.
   */
  def getBoundedStream(fileStream: FSDataInputStream, start: Long, end: Long): FSDataInputStream

  /**
   * Truncates the file up to the given offset.
   */
  def truncateStream(fileStream: FSDataInputStream, position: Long)

  /**
   * Wraps the input stream in a compressed stream.
   */
  def wrapForCompression(blockId: BlockId, fileStream: FSDataInputStream): InputStream

  /**
   * Deletes the file.
   */
  def deleteFile(uri: URI)

  /**
   * Returns an absolute path for the file. This path may or may not already exist.
   * The same path will be returned as long as the file name is same.
   */
  def getTempFilePath(fileName: String): URI

  /**
   * Determines if the file already exists.
   */
  def exists(uri: URI): Boolean

  /**
   * Returns the file size.
   */
  def getFileSize(uri: URI): Long

  /**
   * Returns the disk writer for a given block.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: URI,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter

  /**
   * Returns the data handled by the given disk writer.
   */
  def getValuesFromDiskStore(
    writer: BlockObjectWriter,
    serializer: Serializer): Option[Iterator[Any]]

  /**
   * Returns handle to shuffle client.
   */
  def getShuffleClient(): ShuffleClient

  /**
   * Returns a buffer containing the content of the given file that are within the specified range.
   *
   * @param uri file
   * @param offset starting point for the read
   * @param length bytes to read from starting point
   * @param transportConf
   */
  def createManagedBuffer(
    file: URI,
    offset: Long,
    length: Long,
    transportConf: TransportConf): ManagedBuffer
}

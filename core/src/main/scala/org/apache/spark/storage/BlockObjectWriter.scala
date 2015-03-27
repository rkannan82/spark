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

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel
import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.hadoop.fs.FSDataOutputStream
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream
import java.io.FileNotFoundException

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes. Also, once the writer has
 * been opened, it cannot be reopened again.
 */
private[spark] abstract class BlockObjectWriter(val blockId: BlockId) {

  def open(): BlockObjectWriter

  def close()

  def isOpen: Boolean

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   */
  def revertPartialWritesAndClose()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   */
  def fileSegment(): FileSegment
}

/**
 * BlockObjectWriter which writes directly to a file on disk. Appends to the given file.
 * If the given <code>fileSystem</code> refers to a distributed file system,
 * then the file will be created on it instead of local file system.
 */
private[spark] class DiskBlockObjectWriter(
    blockId: BlockId,
    file: URI,
    serializer: Serializer,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active BlockObjectWriter's who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    fileSystem: FileSystem = new LocalFileSystem)
  extends BlockObjectWriter(blockId)
  with Logging
{
  /** Intercepts write calls and tracks total time spent writing. Not thread safe. */
  private class TimeTrackingOutputStream(out: OutputStream) extends OutputStream {
    def write(i: Int): Unit = callWithTiming(out.write(i))
    override def write(b: Array[Byte]) = callWithTiming(out.write(b))
    override def write(b: Array[Byte], off: Int, len: Int) = callWithTiming(out.write(b, off, len))
    override def close() = out.close()
    override def flush() = out.flush()
  }

  private var bs: OutputStream = null
  private var fos: FSDataOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var hasBeenClosed = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  private val initialPosition: Long = getFileSize
  private var finalPosition: Long = -1
  private var reportedPosition: Long = initialPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0

  private def getFileSize: Long = {
    try {
      fileSystem.getFileSize(file)
    } catch {
      case e: FileNotFoundException => 0
    }
  }

  private def logStats = {
    logDebug("File = %s : Positions = %d:%d:%d".format(file, initialPosition,
      reportedPosition, finalPosition))
  }

  override def open(): BlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }

    fos = fileSystem.create(file, true)
    ts = new TimeTrackingOutputStream(fos)
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializer.newInstance().serializeStream(bs)
    initialized = true
    logStats

    this
  }

  override def close() {
    if (initialized) {
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        objOut.flush()
        def sync = fos.sync()
        callWithTiming(sync)
      }
      objOut.close()

      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
    logStats
  }

  override def isOpen: Boolean = objOut != null

  override def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      close()
    }
    finalPosition = getFileSize
    // In certain compression codecs, more bytes are written after close() is called
    writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
  }

  // Discard current writes. We do this by flushing the outstanding writes and then
  // truncating the file to its initial position.
  override def revertPartialWritesAndClose() {
    try {
      writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition)
      writeMetrics.decShuffleRecordsWritten(numRecordsWritten)

      if (initialized) {
        objOut.flush()
        bs.flush()
        close()
      }

      val truncateStream = fileSystem.open(file)
      try {
        fileSystem.truncateStream(truncateStream, initialPosition)
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
    }
  }

  override def write(value: Any) = {
    if (!initialized) {
      open()
    }

    objOut.writeObject(value)
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
    logStats
  }

  override def fileSegment(): FileSegment = {
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = fos.getPos
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  private def callWithTiming(f: => Unit) = {
    val start = System.nanoTime()
    f
    writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
  }

  // For testing
  private[spark] def flush() {
    objOut.flush()
    bs.flush()
  }
}

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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.network.buffer.DFSManagedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for reading shuffle blocks from a Hadoop Distributed File System.
 */
public class DFSShuffleClient extends ShuffleClient {
  private final Logger logger = LoggerFactory.getLogger(DFSShuffleClient.class);

  private final Configuration conf;
  private final FileSystem fileSystem;
  private String appId;
  private String dfsBaseDir;

  public DFSShuffleClient(Configuration conf) throws IOException {
    this.conf = conf;
    this.fileSystem = FileSystem.get(conf);
    // TODO
    String sparkLocalDir = "/tmp/";
    this.dfsBaseDir = conf.get("fs.defaultFS") + sparkLocalDir;
  }

  @Override
  public void init(String appId) {
    this.appId = appId;
  }

  @Override
  public void fetchBlocks(
      final String host,
      final int port,
      final String execId,
      String[] blockIds,
      BlockFetchingListener listener) {
    assert appId != null : "Called before init()";
    logger.debug("External shuffle fetch from {}:{} (executor id {})", host, port, execId);

    for (String blockId : blockIds) {
     fetchBlock(host, blockId, listener);
    }
  }

  private void fetchBlock(String host, String blockId, BlockFetchingListener listener) {
    logger.debug("Fetching block {}", blockId);

    String[] blockIdParts = blockId.split("_");
    if (blockIdParts.length < 4) {
      throw new IllegalArgumentException("Unexpected block id format: " + blockId);
    } else if (!blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException("Expected shuffle block id, got: " + blockId);
    }

    int shuffleId = Integer.parseInt(blockIdParts[1]);
    int mapId = Integer.parseInt(blockIdParts[2]);
    int reduceId = Integer.parseInt(blockIdParts[3]);

    try {
      DFSManagedBuffer buffer = getBlockData(host, shuffleId, mapId, reduceId);
      listener.onBlockFetchSuccess(blockId, buffer);
    } catch (Exception e) {
      listener.onBlockFetchFailure(blockId, e);
    }
  }

  private DFSManagedBuffer getBlockData(
      String host,
      int shuffleId,
      int mapId,
      int reduceId)
    throws IOException {

    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    Path indexFile = getDFSPath(host, getIndexFile(shuffleId, mapId));
    FSDataInputStream inputStream = fileSystem.open(indexFile);

    try {
      inputStream.skip(reduceId * 8);
      long offset = inputStream.readLong();
      long nextOffset = inputStream.readLong();
      Path dataFile = getDFSPath(host, getDataFile(shuffleId, mapId));

      return new DFSManagedBuffer(dataFile, offset, nextOffset - offset);
    } finally {
      inputStream.close();
    }
  }

  private String getIndexFile(int shuffleId, int mapId) {
    return getShuffleFile(shuffleId, mapId) + ".index";
  }

  private String getDataFile(int shuffleId, int mapId) {
    return getShuffleFile(shuffleId, mapId) + ".data";
  }

  private String getShuffleFile(int shuffleId, int mapId) {
    return "shuffle_" + shuffleId + "_" + mapId + "_0";
  }

  private Path getDFSPath(String host, String fileName) {
    String nodeDirOnDFS = dfsBaseDir + host + "/spark/" + appId;
    Path path = new Path(nodeDirOnDFS + "/" + fileName);
    return path;
  }

  @Override
  public void close() {
  }
}

/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.replication.ReplicationException;

import java.io.IOException;
import java.util.List;

/**
 * Gateway to Replication. Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}. If
 * the peer table is not existent, the retry won't be performed anymore.
 */
public class CrossSiteReplication extends Replication {

  private static final Log LOG = LogFactory.getLog(CrossSiteReplication.class);
  private boolean replication;
  private Configuration conf;
  private Server server;
  private ReplicationSink replicationSink;

  /**
   * Empty constructor
   */
  public CrossSiteReplication() {
  }

  @Override
  public void initialize(final Server server, final FileSystem fs, final Path logDir,
                         final Path oldLogDir) throws IOException {
    this.server = server;
    this.conf = this.server.getConfiguration();
    this.replication = isReplication(this.conf);
    super.initialize(server, fs, logDir, oldLogDir);
  }

  @Override
  public void startReplicationService() throws IOException {
    if (this.replication) {
      try {
        this.getReplicationManager().init();
      } catch (ReplicationException e) {
        LOG.error(e);
      }
      this.replicationSink = new CrossSiteReplicationSink(this.conf, this.server);
    }
  }

  /**
   * Join with the replication threads
   */
  public void join() {
    if (this.replication) {
      this.getReplicationManager().join();
      if (this.replicationSink != null) {
        this.replicationSink.stopReplicationSinkServices();
      }
    }
  }

  /**
   * Carry on the list of log entries down to the sink
   *
   * @param entries list of entries to replicate
   * @throws IOException
   */
  public void replicateLogEntries(List<AdminProtos.WALEntry> entries, CellScanner cells) throws IOException {
    if (this.replication) {
      this.replicationSink.replicateEntries(entries, cells);
    }
  }
}

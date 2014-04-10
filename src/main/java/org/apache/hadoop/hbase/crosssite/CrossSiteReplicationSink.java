/**
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
package org.apache.hadoop.hbase.crosssite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes.TableState;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class is responsible for replicating the edits coming from another cluster. If the peer
 * table is not existent, no retry will be performed.
 * <p/>
 * This replication process is currently waiting for the edits to be applied before the method can
 * return. This means that the replication of edits is synchronized (after reading from HLogs in
 * ReplicationSource) and that a single region server cannot receive edits from two sources at the
 * same time
 * <p/>
 * This class uses the native HBase client in order to replicate entries.
 * <p/>
 * 
 */
public class CrossSiteReplicationSink extends ReplicationSink {

  private static final Log LOG = LogFactory.getLog(CrossSiteReplicationSink.class);
  protected ZooKeeperWatcher zkw;
  protected CrossSiteZNodes znodes;
  protected boolean crossSiteEnabled = false;
  protected Configuration currentClusterConf;

  public CrossSiteReplicationSink(Configuration conf, Stoppable stopper) throws IOException {
    super(conf, stopper);
    String zkConf = conf.get(CrossSiteConstants.CROSS_SITE_ZOOKEEPER);
    if (zkConf != null && !zkConf.trim().equals("")) {
      this.currentClusterConf = conf;
      crossSiteEnabled = true;
      Configuration crossSiteZKConf = new Configuration(conf);
      ZKUtil.applyClusterKeyToConf(crossSiteZKConf, zkConf);
      this.zkw = new ZooKeeperWatcher(crossSiteZKConf, "connection to global zookeeper",
          new CrossSiteDummyAbortable(), false);
      try {
        this.znodes = new CrossSiteZNodes(zkw, false);
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Do the changes and handle the pool. If the table is not found, no retry will be performed.
   * 
   * @param tableName
   *          table to insert into
   * @param rows
   *          list of actions
   * @throws IOException
   */
  @Override
  protected void batch(byte[] tableName, Collection<List<Row>> allRows) throws IOException {
    try {
      super.batch(tableName, allRows);
    } catch (TableNotFoundException e) {
      if (!crossSiteEnabled || !matchCrossSiteTableNamePattern(Bytes.toString(tableName))) {
        throw e;
      }
      // Check whether the cross site table is existent. If the table is not
      // existent, eat the TableNotFoundException. Else, if the table is
      // in a deleting state, eat this exception and log it in debug mode.
      try {
        String cstName = CrossSiteUtil.getCrossSiteTableName(Bytes.toString(tableName));
        boolean tableZNodeExist = znodes.isTableExists(cstName);
        if (tableZNodeExist) {
          TableState state = znodes.getTableState(cstName);
          if (TableState.DELETING.equals(state)) {
            LOG.debug("The cross site table is already DELETING. Not trying to replicate the data",
                e);
          } else {
            // if the state is not deleting, throw the TableNotFoundException
            throw e;
          }
        } else {
          LOG.debug("The cross site table is already deleted. Not trying to replicate the data", e);
        }
      } catch (KeeperException e1) {
        LOG.warn("Fail to get the table state from the global zookeeper", e1);
        // stop this check, and throw the TableNotFoundException
        throw e;
      } catch (IllegalArgumentException e1) {
        LOG.warn(e1);
        throw e;
      }
    } catch (RetriesExhaustedWithDetailsException e) {
      try {
        if (!crossSiteEnabled || !isCrossSiteTable(Bytes.toString(tableName))) {
          throw e;
        }
      } catch (KeeperException e2) {
        LOG.warn("Fail to get the table from the global zookeeper", e2);
        throw e;
      }
      if (!e.mayHaveClusterIssues()
          && e.getMessage().contains(
              "org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException")) {
        try {
          retryBatchAfterRemovingDeletedColumns(tableName, allRows);
        } catch (Exception e1) {
          LOG.warn(e1);
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  private void retryBatchAfterRemovingDeletedColumns(byte[] tableName, Collection<List<Row>> allRows)
      throws Exception {
    String cstTableName = CrossSiteUtil.getCrossSiteTableName(Bytes.toString(tableName));
    HTableDescriptor htd = znodes.getTableDesc(cstTableName);
    Set<byte[]> cfs = htd.getFamiliesKeys();
    List<List<Row>> allRowsForRetry = new ArrayList<List<Row>>();
    // We have a List<List<Row>> and within the super class (ie. ReplicationSink) we
    // iterate over the outer list and give the inner list to HTable#batch(). The moment one such
    // list throws RetriesExhaustedWithDetailsException we will not continue with remaining
    // List<Row>.. So none of the items (the one with only present cfs also) in the remaining lists
    // are replayed. This flag denotes this case. Once this flag is true, even if there is no
    // familyRemoved also we need to add to the new list for retry.
    boolean flag = false;
    Set<byte[]> peerHcds = getHColumnDescriptors(tableName);
    for (List<Row> rows : allRows) {
      List<Row> rowsForRetry = new ArrayList<Row>();
      for (Row row : rows) {
        if (row instanceof Mutation) {
          Mutation m = (Mutation) row;
          Iterator<Entry<byte[], List<KeyValue>>> familyMapItr = m.getFamilyMap().entrySet()
              .iterator();
          boolean familyRemoved = false;
          while (familyMapItr.hasNext()) {
            Entry<byte[], List<KeyValue>> next = familyMapItr.next();
            if (!cfs.contains(next.getKey())) {
              familyMapItr.remove();
              familyRemoved = true;
            } else if (!peerHcds.contains(next.getKey())) {
              throw new IOException("The column doesn't exist in the peer table");
            }
          }
          if ((familyRemoved || flag) && !m.isEmpty()) {
            rowsForRetry.add(m);
          }
        }
      }
      if (!rowsForRetry.isEmpty()) {
        allRowsForRetry.add(rowsForRetry);
        flag = true;
      }
    }
    if (!allRowsForRetry.isEmpty()) {
      super.batch(tableName, allRowsForRetry);
    }
  }

  @Override
  public void stopReplicationSinkServices() {
    super.stopReplicationSinkServices();
    if (this.zkw != null) {
      this.zkw.close();
    }
  }

  private boolean isCrossSiteTable(String tableName) throws KeeperException {
    try {
      String crossSiteTableName = CrossSiteUtil.getCrossSiteTableName(tableName);
      return znodes.isTableExists(crossSiteTableName);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private Set<byte[]> getHColumnDescriptors(byte[] tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(currentClusterConf);
    try {
      HTableDescriptor htd = admin.getTableDescriptor(tableName);
      return htd.getFamiliesKeys();
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the HBaseAdmin", e);
      }
    }
  }

  private boolean matchCrossSiteTableNamePattern(String tableName) {
    int index = tableName.lastIndexOf('_');
    return index > 0 && index < tableName.length() - 1;
  }
}

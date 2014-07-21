/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.crosssite;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.CrossSiteCallable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes.TableState;
import org.apache.hadoop.hbase.crosssite.TableAbnormalStateException;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocatorRPCObject;
import org.apache.hadoop.hbase.crosssite.locator.PrefixClusterLocator;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Strings;

/**
 * Provides an interface to manage cross site tables + general administrative functions. Use
 * CrossSiteHBaseAdmin to create, drop, list, enable and disable cross site tables. Use it also to
 * add and drop table column families.
 */
public class CrossSiteHBaseAdmin implements Abortable {

  public static final String ENABLED_STRING = "enabled";
  public static final String DISABLED_STRING = "disabled";
  private static final Log LOG = LogFactory.getLog(CrossSiteHBaseAdmin.class);

  private ZooKeeperWatcher zkw;
  private CrossSiteZNodes znodes;
  private ExecutorService pool;
  protected Configuration conf;
  private final int numRetries;
  private final long pause;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Mutliply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private boolean aborted;

  public CrossSiteHBaseAdmin(Configuration conf) throws IOException, KeeperException {
//    super();
    // create the connection to the global zk of the CrossSiteHBaseAdmin
    Configuration crossSiteZKConf = new Configuration(conf);
    ZKUtil
        .applyClusterKeyToConf(crossSiteZKConf, conf.get(CrossSiteConstants.CROSS_SITE_ZOOKEEPER));
    this.conf = crossSiteZKConf;
    zkw = new ZooKeeperWatcher(this.conf, "connection to global zookeeper", this, false);
    znodes = new CrossSiteZNodes(zkw);
    this.numRetries = this.conf.getInt("hbase.crosssite.client.retries.number", 5);
    this.retryLongerMultiplier = this.conf.getInt(
        "hbase.crosssite.client.retries.longer.multiplier", 2);
    this.pause = this.conf.getLong("hbase.crosssite.client.pause", 1000);

    int poolSize = this.conf.getInt("hbase.crosssite.admin.pool.size", Integer.MAX_VALUE);
    if (poolSize <= 0) {
      poolSize = Integer.MAX_VALUE;
    }
    final SynchronousQueue<Runnable> blockingQueue = new SynchronousQueue<Runnable>();
    RejectedExecutionHandler rejectHandler = new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
          blockingQueue.put(r);
        } catch (InterruptedException e) {
          throw new RejectedExecutionException(e);
        }
      }
    };
    pool = new ThreadPoolExecutor(1, poolSize, 60, TimeUnit.SECONDS, blockingQueue,
        Threads.newDaemonThreadFactory("crosssite-hbase-admin-"), rejectHandler);
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
  }

  /**
   * Creates a new cross site table. Synchronous operation.
   * 
   * @param desc
   *          table descriptor for table
   * 
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc) throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new cross site table with an initial set of empty regions defined by the specified
   * split keys. The total number of regions created will be the number of split keys plus one.
   * Synchronous operation. Note : Avoid passing empty split key.
   * 
   * @param desc
   *          table descriptor for table
   * @param splitKeys
   *          array of split keys for the initial regions of the table
   * 
   * @throws IllegalArgumentException
   *           if the table name is reserved, if the split keys are repeated and if the split key
   *           has empty byte array.
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    createTable(desc, splitKeys, new PrefixClusterLocator(), true);
  }

  /**
   * Creates a new cross site table with the specified number of regions. The start key specified
   * will become the end key of the first region of the table, and the end key specified will become
   * the start key of the last region of the table (the first region has a null start key and the
   * last region has a null end key).
   * 
   * BigInteger math will be used to divide the key range specified into enough segments to make the
   * required number of total regions.
   * 
   * Synchronous operation.
   * 
   * @param desc
   *          table descriptor for table
   * @param startKey
   *          beginning of key range
   * @param endKey
   *          end of key range
   * @param numRegions
   *          the total number of regions to create
   * 
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws MasterNotRunningException
   *           if master is not running
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    createTable(desc, startKey, endKey, numRegions, new PrefixClusterLocator(), true);
  }

  /**
   * Creates a new cross site table with the specified number of regions. The start key specified
   * will become the end key of the first region of the table, and the end key specified will become
   * the start key of the last region of the table (the first region has a null start key and the
   * last region has a null end key).
   * 
   * BigInteger math will be used to divide the key range specified into enough segments to make the
   * required number of total regions.
   * 
   * Synchronous operation.
   * 
   * @param desc
   *          table descriptor for table
   * @param startKey
   *          beginning of key range
   * @param endKey
   *          end of key range
   * @param numRegions
   *          the total number of regions to create
   * @param locator
   *          cluster locator for table
   * @param createAgainIfAlreadyExists
   *          deletes and creates again if already exists           
   * 
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions,
      ClusterLocator locator,  boolean createAgainIfAlreadyExists) throws IOException {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys, locator, createAgainIfAlreadyExists);
  }

  /**
   * Creates a new cross site table with an initial set of empty regions defined by the specified
   * split keys. The total number of regions created will be the number of split keys plus one.
   * Synchronous operation.
   * 
   * @param desc
   *          table descriptor for table
   * @param splitKeys
   *          array of split keys for the initial regions of the table
   * @param locator
   *          cluster locator for table
   * @param createAgainIfAlreadyExists
   *          deletes the table and creates again if this parameter is true   
   * 
   * @throws TableExistsException
   *           if table already exists (If concurrent threads, the table may have been created
   *           between test-for-existence and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(final HTableDescriptor desc, final byte[][] splitKeys,
      ClusterLocator locator, boolean createAgainIfAlreadyExists) throws IOException {
   // HTableDescriptor.isLegalTableName(desc.getName());
    if (splitKeys != null && splitKeys.length > 1) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      int length = 0;
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, "
              + "found duplicate: " + Bytes.toStringBinary(splitKey) + ", "
              + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
        length += splitKey.length;
        length += WritableUtils.getVIntSize(splitKey.length);
      }
      /*
       * The data in zk node store the boolean(1 byte),int(4 bytes) and split key data. So the
       * length should not be larger than 1048571
       */
      if (length > 1048571) {
        throw new IllegalArgumentException("The split keys should not be larger than 1MB");
      }
    }
    try {
      createTableInternal(desc, splitKeys, locator, createAgainIfAlreadyExists);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Enable a cross site table. May timeout. The table has to be in disabled state for it to be
   * enabled.
   * 
   * @param tableName
   *          name of the table
   * @throws IOException
   *           if a remote or network exception occurs There could be couple types of IOException
   *           TableNotFoundException means the table doesn't exist. TableNotDisabledException means
   *           the table isn't in disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final String tableName) throws IOException {
    try {
      enableTableInternal(tableName);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  /**
   * Enable a cross site table. May timeout. The table has to be in disabled state for it to be
   * enabled.
   * 
   * @param tableName
   *          name of the table
   * @throws IOException
   *           if a remote or network exception occurs There could be couple types of IOException
   *           TableNotFoundException means the table doesn't exist. TableNotDisabledException means
   *           the table isn't in disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(byte[] tableName) throws IOException {
    enableTable(Bytes.toString(tableName));
  }

  /**
   * Enables the cross site tables.
   * 
   * Directly check the state of the znode.
   * 
   * @param tableName
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  private void enableTableInternal(final String tableName) throws IOException, KeeperException,
      InterruptedException, ExecutionException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Enabling the cross site table " + tableName);
    }
    long start = System.currentTimeMillis();
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      boolean locked = false;
      try {
        locked = znodes.lockTable(tableName);
        if (locked) {
          final boolean peersAvailable = isReplicatedTable(znodes.getTableDesc(tableName));
          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }

          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableName);
            } else if (TableState.ENABLING.equals(tableState)
                || TableState.DISABLING.equals(tableState)) {
              LOG.info("Trying to enable a cross site table " + tableName
                  + " in the ENABLING/DISABLING state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }
          Map<String, ClusterInfo> clusterInfo = znodes.listClusterInfos();
          // update the state to enabling
          znodes.setTableState(tableName, TableState.ENABLING);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusterInfo.entrySet()) {
            final String clusterName = entry.getKey();
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
              @Override
              public Void call() throws Exception {
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
                HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue().getAddress());
                try {
                  enableTable(admin, clusterTableName);
                  // should enable on the peers
                  if (peersAvailable) {
                    ClusterInfo ci = entry.getValue();
                    if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                      for (ClusterInfo peer : ci.getPeers()) {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Enabling table " + clusterTableName + " in the peer " + peer);
                        }
                        HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                        String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                            clusterName, peer.getName());
                        try {
                          // When table is not present in the peer cluster, let Exception be thrown
                          // and state of the table continue in ENABLING
                          enableTable(peerAdmin, peerTableName);
                        } finally {
                          try {
                            peerAdmin.close();
                          } catch (IOException e) {
                            LOG.warn("Fail to close the HBaseAdmin of peers", e);
                          }
                        }
                      }
                    }
                  }
                } finally {
                  try {
                    admin.close();
                  } catch (IOException e) {
                    LOG.warn("Fail to close the HBaseAdmin", e);
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              // directly throw the exception.
              result.get();
            }
            // update the znode state
            znodes.setTableState(tableName, TableState.ENABLED);
          } catch (Exception e) {
            LOG.error("Fail to enable the cross site table " + tableName, e);
            throw new IOException(e);
          }
          LOG.info("The cross site table " + tableName + " is enabled");
          return;
        }
      } finally {
        if (locked) {
          znodes.unlockTable(tableName);
        }
      }

      if (tries < this.numRetries * this.retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable enable");
        }
      }
    }
    throw new IOException("Table '" + tableName + "' not yet enabled, after "
        + (System.currentTimeMillis() - start) + "ms.");
  }

  private static void enableTable(HBaseAdmin admin, String tableName) throws IOException {
    try {
      if (!admin.isTableEnabled(tableName)) {
        admin.enableTable(tableName);
      }
    } catch (TableNotDisabledException e) {
      // Supress the TableNotDisabledException.
    }
  }

  private static void disableTable(HBaseAdmin admin, String tableName) throws IOException {
    try {
      if (!admin.isTableDisabled(tableName)) {
        admin.disableTable(tableName);
      }
    } catch (TableNotEnabledException e) {
      // Supress the TableNotEnabledException.
    }
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is on-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if table is on-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName, boolean perClusterCheck) throws IOException {
    return isTableEnabled(Bytes.toString(tableName), perClusterCheck);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is on-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if table is on-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName, boolean perClusterCheck) throws IOException {
    try {
      return isTableEnabledInternal(tableName, perClusterCheck);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is off-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return isTableDisabled(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if table is off-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName, boolean perClusterCheck) throws IOException {
    return isTableDisabled(Bytes.toString(tableName), perClusterCheck);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is off-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(String tableName) throws IOException {
    return isTableDisabled(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if table is off-line
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(String tableName, boolean perClusterCheck) throws IOException {
    try {
      return isTableDisabledInternal(tableName, perClusterCheck);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if all regions of the table are available
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if all regions of the table are available
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName, boolean perClusterCheck) throws IOException {
    try {
      return isTableAvailableInternal(tableName, perClusterCheck);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if all regions of the table are available
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return isTableAvailable(tableName, false);
  }

  /**
   * @param tableName
   *          name of table to check
   * @param perClusterCheck
   * @return true if all regions of the table are available
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName, boolean perClusterCheck) throws IOException {
    return isTableAvailable(Bytes.toString(tableName), perClusterCheck);
  }

  /**
   * List all the cross site tables.
   * 
   * @return - returns an array of HTableDescriptors
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException {
    try {
      return znodes.listTableDescs();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * List all the cross site tables matching the given pattern.
   * 
   * @param pattern
   *          The compiled regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException
   *           if a remote or network exception occurs
   * @see #listTables()
   */
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    try {
      return znodes.listTableDescs(pattern);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * List all the tables matching the given regular expression.
   * 
   * @param regex
   *          The regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException
   *           if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }

  /**
   * Adds the cluster into the zookeeper. Meanwhile, checks whether there're cross-site tables, if
   * yes, to create those tables in the current cluster.
   * 
   * @param name
   * @param address
   * @throws IOException
   */
  public void addCluster(final String name, final String address) throws IOException {
    this.addCluster(name, address, true);
  }

  /**
   * Adds the cluster into the zookeeper. Meanwhile, checks whether there're cross-site tables, if
   * yes, to create those tables in the current cluster.
   * 
   * @param name
   * @param address
   * @param createTableAgainIfAlreadyExists
   *          if true, delete the table and recreate it. If false, just enable it if the table is
   *          existent, create it if the table is not existent.
   * @throws IOException
   */
  public void addCluster(final String name, final String address,
      final boolean createTableAgainIfAlreadyExists)
      throws IOException {
    CrossSiteUtil.validateClusterName(name);
    ZKUtil.transformClusterKey(address); // This is done to validate the address.
    Map<String, ClusterInfo> clusters = null;
    try {
      clusters = znodes.listClusterInfos();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    if (clusters.get(name) != null) {
      throw new IOException("A Cluster with same name '" + name + "' already available");
    }
    for (ClusterInfo cluster : clusters.values()) {
      if (cluster.getAddress().equals(address)) {
        throw new IOException("A cluster " + cluster + " with same address already available");
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding cluster " + name + ":" + address);
    }
    HBaseAdmin admin = null;
    try {
      // Add all the existing tables to this cluster
      String[] tableNames = znodes.getTableNames();
      if (tableNames != null && tableNames.length > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating tables " + toString(tableNames) + " in the cluster " + name);
        }
        admin = createHBaseAmin(conf, address);
        List<Future<Void>> results = new ArrayList<Future<Void>>();
        for (final String tableName : tableNames) {
          final String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, name);
          results.add(pool.submit(new CrossSiteCallable<Void>(admin) {
            @Override
            public Void call() throws Exception {
              int tries = 0;
              for (; tries < numRetries * retryLongerMultiplier; ++tries) {
                if (znodes.lockTable(tableName)) {
                  try {
                    HTableDescriptor htd = znodes.getTableDescAllowNull(tableName);
                    if (htd == null) {
                      return null;
                    }
                    htd.setName(Bytes.toBytes(clusterTableName));
                    byte[][] splitKeys = getTableSplitsForCluster(tableName, name);
                    boolean needCreate = true;
                    if (hbaseAdmin.tableExists(clusterTableName)) {
                      if (createTableAgainIfAlreadyExists) {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Table " + clusterTableName
                              + " already present in the cluster " + name
                              + ". Going to drop it and create again");
                        }
                        disableTable(hbaseAdmin, clusterTableName);
                        hbaseAdmin.deleteTable(clusterTableName);
                      } else {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Table " + clusterTableName
                              + " already present in the cluster " + name + ". Going to enable it");
                        }
                        enableTable(hbaseAdmin, clusterTableName);
                        needCreate = false;
                      }
                    }
                    if (needCreate) {
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating table " + clusterTableName + " in the cluster " + name);
                      }
                      hbaseAdmin.createTable(htd, splitKeys);
                    }
                  } finally {
                    znodes.unlockTable(tableName);
                  }
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Created table " + clusterTableName + " in the cluster " + name);
                  }
                  return null;
                }
                if (tries < numRetries * retryLongerMultiplier - 1) {
                  try { // Sleep
                    Thread.sleep(getPauseTime(tries));
                  } catch (InterruptedException e) {
                    throw new InterruptedIOException("Interrupted when waiting"
                        + " for cross site HTable enable");
                  }
                }
              }
              // All retries for acquiring locks failed! Throwing Exception
              throw new RetriesExhaustedException("Not able to acquire table lock after " + tries
                  + " tries");
            }
          }));
        }
        for (Future<Void> result : results) {
          result.get();
        }

        LOG.info("The tables in the cluster " + name + " are created");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Create the znode of the cluster " + name);
      }
      znodes.createClusterZNode(name, address);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          LOG.warn("Fail to close the HBaseAdmin", e);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The cluster " + name + ":" + address + " is created");
    }
  }

  private String toString(Object[] objs) {
    StringBuilder sb = new StringBuilder();
    for (Object obj : objs) {
      sb.append(obj);
    }
    return sb.toString();
  }

  /**
   * Deletes the cluster.
   * 
   * @param clusterName
   * @throws IOException
   */
  public void deleteCluster(String clusterName) throws IOException {
    // TODO - Do we need to delete all the CSBT tables created in this cluster also?
    try {
      // Check if valid cluster name.  If not throw error.  Otherwise from shell script it would be difficult 
      // to know if the user is specifying a valid clustername
      if(znodes.getClusterInfo(clusterName) == null) {
        LOG.error("Invalid cluster name "+clusterName);
        throw new IOException("Invalid cluster name "+clusterName);
      }
      znodes.deleteClusterZNode(clusterName);
    } catch (KeeperException e) {
      LOG.error("Fail to delete the cluster " + clusterName, e);
      throw new IOException(e);
    }
  }

  /**
   * Add a peer for this cluster. Creates the replicated tables in the peer.
   * 
   * @param clusterName
   * @param peer
   *          Peer cluster. Add the peer cluster name as first item in the Pair and peer cluster
   *          address as second item.
   * @throws IOException
   */
  public void addPeer(final String clusterName, final Pair<String, String> peer)
      throws IOException {
    if (peer == null || Strings.isNullOrEmpty(peer.getFirst())
        || Strings.isNullOrEmpty(peer.getSecond())) {
      throw new IllegalArgumentException("Peer should be specified");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding peer " + peer + " to the cluster " + clusterName);
    }
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      boolean locked = false;
      try {
        locked = znodes.lockCluster(clusterName);
        if(locked) {
          // STEP 0 : Check whether the cluster name is existent, and whether the peer is existent.
          String clusterAddress = znodes.getClusterAddress(clusterName);
          if (clusterAddress == null) {
            throw new IllegalArgumentException("The cluster[" + clusterName + "] doesn't exist");
          }
          if (znodes.isPeerExists(clusterName, peer.getFirst())) {
            throw new IllegalArgumentException("The peer[" + peer + "] has been existent");
          }
          // STEP 1 : Create all the replicated tables of source cluster in the peer cluster
          if (LOG.isDebugEnabled()) {
            LOG.debug("Creating replicated tables of " + clusterName + " in peer " + peer);
          }
          String[] tableNames = znodes.getTableNames();
          if (tableNames != null && tableNames.length > 0) {
            createTablesInPeer(clusterName, tableNames, peer);
          }
          // STEP 2 : Adding the peer to the peer list in zookeeper
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding " + peer + " to zk peer list for cluster " + clusterName);
          }
          znodes.createPeer(clusterName, peer);
          LOG.debug("Added " + peer + " to zk peer list for cluster " + clusterName);
          // STEP 3 : Add the peer to master cluster's replication peer list
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding " + peer + " to cluster " + clusterName + "'s replication peer list");
          }
          addReplicationPeer(clusterName, peer);
          LOG.info("Added " + peer + " to cluster " + clusterName + "'s replication peer list");

          LOG.info("The peer " + peer + " is added for cluster " + clusterName);
          return;
        }
      } catch (KeeperException e) {
        throw new IOException(e);
      } finally {
        if (locked) {
          znodes.unlockCluster(clusterName);
        }
      }
      if (tries < this.numRetries * this.retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site table peer creation");
        }
      }
    }
    // throw an exception
    throw new IOException("Retries exhausted while still waiting for the peer of the cluster: "
        + clusterName + " to be created");
  }

  private void createTablesInPeer(final String masterCluster, final String[] tableNames,
      final Pair<String, String> peer) throws KeeperException, IOException {
    List<Future<Void>> results = new ArrayList<Future<Void>>();
    for (final String tableName : tableNames) {
      results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
        @Override
        public Void call() throws Exception {
          final HTableDescriptor htdFormZk = znodes.getTableDescAllowNull(tableName);
          if (htdFormZk == null) {
            return null;
          }
          boolean replicationEnabled = isReplicatedTable(htdFormZk);
          if (replicationEnabled) {
            HTableDescriptor htd = new HTableDescriptor(htdFormZk);
            HColumnDescriptor[] hcds = htd.getColumnFamilies();
            for (HColumnDescriptor hcd : hcds) {
              // only create the CFs that have the scope as 1.
              if (hcd.getScope() > 0) {
                hcd.setScope(0);
              } else {
                htd.removeFamily(hcd.getName());
              }
            }
            byte[][] splitKeys = getTableSplitsForCluster(tableName, masterCluster);
            String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                masterCluster, peer.getFirst());
            htd.setName(Bytes.toBytes(peerTableName));
            HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getSecond());
            try {
              // TODO : Should ensure that the existing data is copied
              if (peerAdmin.tableExists(peerTableName)) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Table " + peerTableName + " already present in the peer "
                      + peer + " Going to drop it and create again");
                }
                disableTable(peerAdmin, peerTableName);
                peerAdmin.deleteTable(peerTableName);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Creating table " + peerTableName + " in the peer cluster "
                    + peer);
              }
              peerAdmin.createTable(htd, splitKeys);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Created table " + peerTableName + " in the peer cluster " + peer);
              }
            } finally {
              if (peerAdmin != null) {
                try {
                  peerAdmin.close();
                } catch (IOException e) {
                  LOG.warn("Fail to close the HBaseAdmin", e);
                }
              }
            }
          }
          return null;
        }
      }));
    }
    try {
      for (Future<Void> result : results) {
        result.get();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  private void addReplicationPeer(String clusterName, final Pair<String, String> peer)
      throws IOException {
    String address;
    try {
      address = znodes.getClusterAddress(clusterName);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    Configuration clusterConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(clusterConf, address);
    // Ensure that we should set this.
    clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationAdmin rAdmin = new ReplicationAdmin(clusterConf);
    try {
      Map<String, String> peerMap = rAdmin.listPeers();
      if (!peerMap.containsKey(peer.getFirst())) {
        rAdmin.addPeer(peer.getFirst(), peer.getSecond());
      } else {
        boolean peerState = rAdmin.getPeerState(peer.getFirst());
        if (!peerState) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Peer " + peer + " for cluster " + clusterName
                + " is in disabled state now. Enabling the same.");
          }
          rAdmin.enablePeer(peer.getFirst());
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(peer + " is an active peer for cluster " + clusterName + " already!");
          }
        }
      }
    } catch (ReplicationException e) {
      LOG.error(e);
    } finally {
      try {
        rAdmin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the ReplicationAdmin", e);
      }
    }
  }

  /**
   * Deletes all the peers of the cluster, meanwhile delete the replicated tables in the peers.
   * 
   * @param clusterName
   * @throws IOException
   */
  public void deletePeers(final String clusterName) throws IOException {
    this.deletePeers(clusterName, null);
  }

  private void removeReplicationPeers(final String clusterName, List<ClusterInfo> peers)
      throws IOException {
    String clusterAddress = null;
    try {
      clusterAddress = znodes.getClusterAddress(clusterName);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    Configuration clusterConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationAdmin rAdmin = new ReplicationAdmin(clusterConf);
    try {
      Map<String, String> peerMap = rAdmin.listPeers();
      for (ClusterInfo peer : peers) {
        if (peerMap.containsKey(peer.getName())) {
          try {
            rAdmin.removePeer(peer.getName());
          } catch (ReplicationException e) {
            LOG.error(e);
          }
        }
      }
    } finally {
      try {
        rAdmin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the ReplicationAdmin", e);
      }
    }
  }

  /**
   * Deletes the peers of the cluster, meanwhile delete the replicated tables in these peers.
   * 
   * @param clusterName
   * @param peerNames
   * @throws IOException
   */
  public void deletePeers(final String clusterName, String[] peerNames) throws IOException {
    List<ClusterInfo> peers = null;
    List<String> tableNames = null;
    try {
      peers = znodes.getClusterPeers(clusterName, peerNames);
      tableNames = znodes.listTables();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    if (peers != null && !peers.isEmpty()) {
      // STEP 1 : Remove these peers from master cluster's replication peer list
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing " + peers + " from cluster " + clusterName + "'s replication peer list");
      }
      removeReplicationPeers(clusterName, peers);
      LOG.info("Removed " + peers + " from cluster " + clusterName + "'s replication peer list");

      // STEP 2 : Removing the peers from the peer list in zookeeper
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing " + peers + " from zk peer list for cluster " + clusterName);
      }
      try {
        znodes.deletePeers(clusterName, CrossSiteUtil.toStringList(peers).toArray(new String[0]));
      } catch (KeeperException e) {
        throw new IOException(e);
      }
      LOG.info("Removed " + peers + " from zk peer list for cluster " + clusterName);

      // STEP 3 : Drop all the replicated tables in these peer clusters
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dropping tables replicated from " + clusterName + " in peers " + peers);
      }
      if (tableNames != null && !tableNames.isEmpty()) {
        deleteTablesFromPeers(clusterName, tableNames, peers);
      }
      LOG.info("Dropped tables replicated from " + clusterName + " in peers " + peers);
    }
  }
  
  /**
   * Disables replication on the given peer that belongs to the specified cluster.
   * The replication should be previously enabled to disable.
   * @param clusterName
   * @param peerName
   * @throws IOException
   */
  public void disablePeer(String clusterName, String peerName) throws IOException {
    if(peerName == null) {
      throw new IllegalArgumentException("The peerName cannot be null");
    }
    List<ClusterInfo> clusterPeers = null;
    String peer[]  = new String[1];
    peer[0] = peerName;
    try {
      clusterPeers = znodes.getClusterPeers(clusterName, peer);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    if(clusterPeers == null || clusterPeers.isEmpty()) {
      LOG.error("The peerName" + peerName+ " does not exist");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disabling " + peerName + " from cluster " + clusterName + "'s replication peer list");
      }
      disableReplicationPeer(clusterName, clusterPeers);
      LOG.info("Disabled " + peerName + " from cluster " + clusterName + "'s replication peer list");
    }
  }
  
  private void disableReplicationPeer(String clusterName, List<ClusterInfo> clusterPeers)
      throws IOException {
    String clusterAddress = null;
    try {
      clusterAddress = znodes.getClusterAddress(clusterName);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    Configuration clusterConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationAdmin rAdmin = new ReplicationAdmin(clusterConf);
    try {
      Map<String, String> peerMap = rAdmin.listPeers();
      for (ClusterInfo peer : clusterPeers) {
        if (peerMap.containsKey(peer.getName())) {
            boolean peerState = rAdmin.getPeerState(peer.getName());
            if (peerState) {
              rAdmin.disablePeer(peer.getName());
            } else {
            LOG.error("The peer " + peer.getName() + " Not in ENABLED state.");
          }
        }
      }
    } catch (ReplicationException e) {
      LOG.error(e);
    } finally {
      try {
        rAdmin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the ReplicationAdmin", e);
      }
    }
  }
  
  /**
   * Enables replication on the given peer that belongs to the specified cluster.
   * The replication should be previously disabled to enable.
   * @param clusterName
   * @param peerName
   * @throws IOException
   */
  public void enablePeer(String clusterName, String peerName) throws IOException {
    if(peerName == null) {
      throw new IllegalArgumentException("The peerName cannot be null");
    }
    List<ClusterInfo> clusterPeers = null;
    String peer[]  = new String[1];
    peer[0] = peerName;
    try {
      clusterPeers = znodes.getClusterPeers(clusterName, peer);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    if(clusterPeers == null || clusterPeers.isEmpty()) {
      LOG.error("The peerName" + peerName+ " does not exist");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Enabling " + peerName + " from cluster " + clusterName + "'s replication peer list");
      }
      enableReplicationPeer(clusterName, clusterPeers);
      LOG.info("Enabled " + peerName + " from cluster " + clusterName + "'s replication peer list");
    }
  }
  
  private void enableReplicationPeer(String clusterName, List<ClusterInfo> clusterPeers)
      throws IOException {
    String clusterAddress = null;
    try {
      clusterAddress = znodes.getClusterAddress(clusterName);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    Configuration clusterConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationAdmin rAdmin = new ReplicationAdmin(clusterConf);
    try {
      Map<String, String> peerMap = rAdmin.listPeers();
      for (ClusterInfo peer : clusterPeers) {
        if (peerMap.containsKey(peer.getName())) {
          boolean peerState = rAdmin.getPeerState(peer.getName());
          if (peerState) {
            rAdmin.enablePeer(peer.getName());
          } else {
            LOG.error("The peer " + peer.getName() + " Not in DISABLED state.");
          }
        }
      }
    } catch (ReplicationException e) {
      LOG.error(e);
    } finally {
      try {
        rAdmin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the ReplicationAdmin", e);
      }
    }
  }

  private void deleteTablesFromPeers(final String masterCluster, final List<String> tableNames,
      final List<ClusterInfo> peers) throws IOException {
    List<Future<Void>> results = new ArrayList<Future<Void>>();
    for (final ClusterInfo peer : peers) {
      results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
        @Override
        public Void call() throws Exception {
          HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
          try {
            for (String tableName : tableNames) {
              String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, masterCluster);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping table " + clusterTableName + " from the peer " + peer);
              }
              if (peerAdmin.tableExists(clusterTableName)) {
                if (!peerAdmin.isTableDisabled(clusterTableName)) {
                  peerAdmin.disableTable(clusterTableName);
                }
                peerAdmin.deleteTable(clusterTableName);
              }
            }
          } finally {
            try {
              peerAdmin.close();
            } catch (IOException e) {
              LOG.warn("Fail to close the HBaseAdmin of the peer " + peer.getAddress(), e);
            }
          }
          return null;
        }
      }));
    }
    try {
      for (Future<Void> result : results) {
        result.get();
      }
    } catch (Exception e) {
      // do nothing. Even the exception occurs, we regard the cross site
      // HTable has been deleted
      LOG.error("Fail to delete the peers of the cluster " + masterCluster, e);
      throw new IOException(e);
    }
  }

  /**
   * Gets all the clusters.
   * 
   * @return the array of the cluster names.
   * @throws IOException
   */
  public ClusterInfo[] listClusters() throws IOException {
    try {
      Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
      return clusters.values().toArray(new ClusterInfo[0]);
    } catch (KeeperException e) {
      LOG.error("Fail to list the clusters", e);
      throw new IOException(e);
    }
  }

  /**
   * Gets the peers of a cluster.
   * 
   * @param clusterName
   * @return
   * @throws IOException
   */
  public ClusterInfo[] listPeers(String clusterName) throws IOException {
    try {
      List<ClusterInfo> peers = znodes.getPeerClusters(clusterName);
      return peers.toArray(new ClusterInfo[0]);
    } catch (KeeperException e) {
      LOG.error("Fail to list the peers of the cluster " + clusterName);
      throw new IOException(e);
    }
  }

  /**
   * Adds the hierarchy.
   * 
   * @param parent
   * @param children
   * @throws IOException
   */
  public void addHierarchy(String parent, String[] children) throws IOException {
    try {
      // TODO : This method should have some prechecks. Passing any string as parent is fine here?
      // The parent should be a cluster already present. If not throw error?
      znodes.createHierarchy(parent, children);
    } catch (KeeperException e) {
      LOG.error("Fail to add the hierarchy", e);
      throw new IOException(e);
    }
  }

  /**
   * Deletes all the hierarchy of this cluster.
   * 
   * @param parent
   * @throws IOException
   */
  public void deleteHierarchy(String parent) throws IOException {
    deleteHierarchy(parent, null);
  }

  /**
   * Deletes part of the hierarchy.
   * 
   * @param parent
   * @param children
   * @throws IOException
   */
  public void deleteHierarchy(String parent, String[] children) throws IOException {
    try {
      znodes.deleteHierarchy(parent, children);
    } catch (KeeperException e) {
      LOG.error("Fail to delete the hierarchy of " + parent);
      throw new IOException(e);
    }
  }

  /**
   * Lists all the names of the child clusters.
   * 
   * @param parent
   * @return
   * @throws IOException
   */
  public String[] listChildClusters(String parent) throws IOException {
    try {
      Map<String, Set<String>> hierarchyMap = znodes.getHierarchyMap();
      Set<String> children = CrossSiteUtil.getChildClusters(hierarchyMap, parent);
      return children.toArray(new String[0]);
    } catch (KeeperException e) {
      LOG.error("Fail to list the child clusters of " + parent);
      throw new IOException(e);
    }
  }

  /**
   * Lists all the names of the descendant clusters.
   * 
   * @param parent
   * @return
   * @throws IOException
   */
  public String[] listDescendantClusters(String parent) throws IOException {
    try {
      Map<String, Set<String>> hierarchyMap = znodes.getHierarchyMap();
      Set<String> children = CrossSiteUtil.getDescendantClusters(hierarchyMap, parent);
      return children.toArray(new String[0]);
    } catch (KeeperException e) {
      LOG.error("Fail to list the descendant clusters of " + parent);
      throw new IOException(e);
    }
  }

  /**
   * @return cluster status
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    try {
      List<String> clusterNames = znodes.listClusters();
      List<String> addresses = new ArrayList<String>();
      if (clusterNames != null) {
        for (String clusterName : clusterNames) {
          addresses.add(znodes.getClusterAddress(clusterName));
        }
      }
      List<Future<ClusterStatus>> results = new ArrayList<Future<ClusterStatus>>();
      for (final String address : addresses) {
        results.add(pool.submit(new CrossSiteCallable<ClusterStatus>(conf) {

          @Override
          public ClusterStatus call() throws Exception {
            Configuration clusterConf = new Configuration(configuration);
            ZKUtil.applyClusterKeyToConf(clusterConf, address);
            HBaseAdmin admin = null;
            try {
              admin = new HBaseAdmin(clusterConf);
              return admin.getClusterStatus();
            } finally {
              if (admin != null) {
                try {
                  admin.close();
                } catch (IOException e) {
                  LOG.warn("Fail to close the HBaseAdmin", e);
                }

              }
            }
          }

        }));
      }
      List<ClusterStatus> status = new ArrayList<ClusterStatus>();
      for (Future<ClusterStatus> result : results) {
        try {
          status.add(result.get());
        } catch (ExecutionException e) {
          LOG.error("Fail to get the cluster status", e);
          throw new IOException(e);
        } catch (InterruptedException e) {
          LOG.error("Fail to get the cluster status", e);
          throw new IOException(e);
        }
      }
      return new CrossSiteClusterStatus(status);
    } catch (KeeperException e) {
      LOG.error("Fail to get the cluster status", e);
      throw new IOException(e);
    }

  }

  /**
   * Only flushes a table. Synchronous operation.
   * 
   * @param tableName
   *          table to flush
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final String tableName) throws IOException, InterruptedException {
    // only flush the table
    try {
      // Do table existence check
      znodes.getTableState(tableName);
      // call HBaseAdmin.flush
      Map<String, String> clusterAddresses = znodes.listClusterAddresses();
      // access the cluster one by one
      List<Future<Void>> results = new ArrayList<Future<Void>>();
      for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
        results.add(pool.submit(new CrossSiteCallable<Void>(conf) {

          @Override
          public Void call() throws Exception {
            Configuration clusterConf = new Configuration(configuration);
            ZKUtil.applyClusterKeyToConf(clusterConf, entry.getValue());
            String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, entry.getKey());
            HBaseAdmin admin = new HBaseAdmin(clusterConf);
            try {
              admin.flush(clusterTableName);
            } finally {
              try {
                admin.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin.", e);
              }
            }
            return null;
          }
        }));
      }
      for (Future<Void> result : results) {
        try {
          result.get();
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Only flushes a table. Synchronous operation.
   * 
   * @param tableName
   *          table to flush
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(byte[] tableName) throws IOException, InterruptedException {
    // only flush the table
    flush(Bytes.toString(tableName));
  }

  /**
   * Get tableDescriptors
   * 
   * @param tableNames
   *          List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public HTableDescriptor[] getTableDescriptors(List<String> tableNames) throws IOException {
    try {
      return znodes.listTableDescs(tableNames);
    } catch (KeeperException e) {
      LOG.error("Fail to get the HTableDescriptors", e);
      throw new IOException(e);
    }
  }

  /**
   * Disable the cross site table and wait on completion. The table has to be in enabled state for
   * it to be disabled.
   * 
   * @param tableName
   * @throws IOException
   *           There could be couple types of IOException TableNotFoundException means the table
   *           doesn't exist. TableNotEnabledException means the table isn't in enabled state.
   */
  public void disableTable(String tableName) throws IOException {
    try {
      disableTableInternal(tableName);
    } catch (Exception e) {
      LOG.error("Fail to disable the table " + tableName, e);
      throw new IOException(e);
    }
  }

  /**
   * Disable the cross site table and wait on completion. The table has to be in enabled state for
   * it to be disabled.
   * 
   * @param tableName
   * @throws IOException
   *           There could be couple types of IOException TableNotFoundException means the table
   *           doesn't exist. TableNotEnabledException means the table isn't in enabled state.
   */
  public void disableTable(byte[] tableName) throws IOException {
    disableTable(Bytes.toString(tableName));
  }

  /**
   * Disables the cross site table.
   * 
   * @param tableName
   * @throws Exception
   */
  private void disableTableInternal(final String tableName) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start to disable the cross site table " + tableName);
    }
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      // create a ephemeral node
      boolean locked = false;
      try {
        locked = znodes.lockTable(tableName);
        if (locked) {
          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }
          if (!TableState.ENABLED.equals(tableState)) {
            if (TableState.DISABLED.equals(tableState)) {
              throw new TableNotEnabledException(tableName);
            } else if (TableState.ENABLING.equals(tableState)
                || TableState.DISABLING.equals(tableState)) {
              LOG.info("Try to disable a cross site table " + tableName
                  + " in the ENABLING/DISABLING state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }
          // call HBaseAdmin.disableTable
          Map<String, ClusterInfo> clusterInfo = znodes.listClusterInfos();
          // update the state to disabling
          znodes.setTableState(tableName, TableState.DISABLING);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusterInfo.entrySet()) {
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {

              @Override
              public Void call() throws Exception {
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableName,
                    entry.getKey());
                HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue().getAddress());
                try {
                  disableTable(admin, clusterTableName);
                } finally {
                  try {
                    admin.close();
                  } catch (IOException e) {
                    LOG.warn("Fail to close the HBaseAdmin", e);
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              // directly throw the exception.
              result.get();
            }
            // update the znode state
            znodes.setTableState(tableName, TableState.DISABLED);
          } catch (Exception e) {
            LOG.error("Fail to disable the cross site table " + tableName, e);
            throw new IOException(e);
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("The cross site table " + tableName + " is disabled");
          }
          return;
        }
      } finally {
        if (locked) {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < this.numRetries * this.retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable disable");
        }
      }
    }
    throw new IOException("Retries exhausted, it took too long to wait" + " for the table "
        + tableName + " to be disabled.");
  }

  /**
   * Deletes a table. Synchronous operation.
   * 
   * @param tableName
   *          name of table to delete
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void deleteTable(String tableName) throws IOException {
    try {
      deleteTableInternal(tableName);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a table. Synchronous operation.
   * 
   * @param tableName
   *          name of table to delete
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void deleteTable(byte[] tableName) throws IOException {
    deleteTable(Bytes.toString(tableName));
  }

  /**
   * Deletes a cross site table, meanwhile delete all its replicated tables.
   * 
   * @param tableName
   * @throws Exception
   */
  private void deleteTableInternal(final String tableName) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start to delete the cross site table " + tableName);
    }
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      boolean locked = false;
      try {
        locked = znodes.lockTable(tableName);
        if (locked) {
          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }
          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableName);
            } else if (TableState.DELETING.equals(tableState)) {
              LOG.info("Try to delete the cross site table " + tableName + " in the DELETING state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }
          Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
          // update the table state to deleting
          znodes.setTableState(tableName, TableState.DELETING);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusters.entrySet()) {
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {

              @Override
              public Void call() throws Exception {
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableName,
                    entry.getKey());
                HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue().getAddress());
                try {
                  if (admin.tableExists(clusterTableName)) {
                    if (!admin.isTableDisabled(clusterTableName)) {
                      admin.disableTable(clusterTableName);
                    }
                    admin.deleteTable(clusterTableName);
                  }
                } finally {
                  try {
                    admin.close();
                  } catch (IOException e) {
                    LOG.warn("Fail to close the HBaseAdmin", e);
                  }
                }

                // remove the tables in peers.
                ClusterInfo ci = entry.getValue();
                if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                  for (ClusterInfo peer : ci.getPeers()) {
                    LOG.info("Start to delete the table " + clusterTableName + " from the peer "
                        + peer.getAddress());
                    HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                    try {
                      if (peerAdmin.tableExists(clusterTableName)) {
                        if (!peerAdmin.isTableDisabled(clusterTableName)) {
                          peerAdmin.disableTable(clusterTableName);
                        }
                        peerAdmin.deleteTable(clusterTableName);
                      }
                    } finally {
                      try {
                        peerAdmin.close();
                      } catch (IOException e) {
                        LOG.warn("Fail to close the HBaseAdmin of the peer " + peer.getAddress(), e);
                      }
                    }
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            // remove the znodes to the {tableName}.
            znodes.deleteTableZNode(tableName);
          } catch (Exception e) {
            LOG.error("Fail to delete the cross site table " + tableName, e);
            throw new IOException(e);
          }
          LOG.info("The znode of " + tableName + " is deleted");
          return;
        }
      } finally {
        if (locked) {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < this.numRetries * this.retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site table delete");
        }
      }
    }
    // throw an exception
    throw new IOException("Retries exhausted while still waiting for table: " + tableName
        + " to be delete");
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Creates a cross site table. If the table need be replicated to the peers, this table will
   * created in the peers.
   * 
   * @param desc
   * @param splitKeys
   * @param locator
   * @param createAgainIfAlreadyExists
   * @throws Exception
   */
  private void createTableInternal(final HTableDescriptor desc, final byte[][] splitKeys,
      final ClusterLocator locator, boolean createAgainIfAlreadyExists) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start to create the cross site table " + desc.getNameAsString());
    }
    final String tableName = desc.getNameAsString();
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      if (znodes.isTableStateExist(tableName)) {
        throw new TableExistsException(tableName);
      }
      boolean locked = false;
      try {
        locked = znodes.lockTable(tableName);
        if (locked) {
          if (znodes.isTableStateExist(tableName)) {
            throw new TableExistsException(tableName);
          }
          final boolean createTableInPeers = isReplicatedTable(desc);
          Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
          znodes.createTableZNode(tableName);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          createTableOnClusters(desc, splitKeys, locator, tableName, createTableInPeers, clusters,
              results, createAgainIfAlreadyExists);
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            LOG.info("The cross site table " + desc.getNameAsString() + " is created");
            // add the znodes to the {tableName}.
            addTableChildrenZNodes(desc, splitKeys, locator);
            return;
          } catch (Exception e) {
            LOG.error("Fail to create the cross site table:" + tableName, e);
            // remove the table znode
            // leave all the created HTables
            znodes.deleteTableZNode(tableName);
            throw new IOException(e);
          }
        }
      } finally {
        if (locked) {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < this.numRetries * this.retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site table creation");
        }
      }
    }
    // throw an exception
    throw new IOException("Retries exhausted while still waiting for table: " + tableName
        + " to be created");
  }

  protected void createTableOnClusters(final HTableDescriptor desc, final byte[][] splitKeys,
      final ClusterLocator locator, final String tableName, final boolean createTableInPeers,
      Map<String, ClusterInfo> clusters, List<Future<Void>> results, final boolean createAgainIfAlreadyExists) {
    for (final Entry<String, ClusterInfo> entry : clusters.entrySet()) {
      final String clusterName = entry.getKey();
      results.add(pool.submit(new CrossSiteCallable<Void>(conf) {

        @Override
        public Void call() throws Exception {
          String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
          HTableDescriptor htd = new HTableDescriptor(desc);
          htd.setName(Bytes.toBytes(clusterTableName));
          byte[][] newSplitKeys = locator.getSplitKeys(clusterName, splitKeys);
          HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue().getAddress());
          try {
            boolean tableExists = admin.tableExists(clusterTableName);
            if (tableExists) {
              if (createAgainIfAlreadyExists) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Table " + clusterTableName + " already present in the cluster "
                      + clusterName + ". Going to drop it and create again");
                }
                disableTable(admin, clusterTableName);
                admin.deleteTable(clusterTableName);
                createTable(clusterName, admin, clusterTableName, htd, newSplitKeys);
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Table " + clusterTableName + " already present in the cluster "
                      + clusterName + ". Going to enable it");
                }
                enableTable(admin, clusterTableName);
              }
            } else {
              createTable(clusterName, admin, clusterTableName, htd, newSplitKeys);
            }
            
            // creates the table in peers.
            if (createTableInPeers) {
              ClusterInfo ci = entry.getValue();
              HTableDescriptor peerHtd = new HTableDescriptor(desc);
              for (HColumnDescriptor hcd : peerHtd.getColumnFamilies()) {
                // only create the CFs that have the scope as 1.
                if (hcd.getScope() > 0) {
                  hcd.setScope(0);
                } else {
                  peerHtd.removeFamily(hcd.getName());
                }
              }
              if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                for (ClusterInfo peer : ci.getPeers()) {
                  HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                  String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                      clusterName, peer.getName());
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating table " + peerTableName + " at peer " + peer);
                  }
                  try {
                    boolean peerTableExists = peerAdmin.tableExists(peerTableName);
                    peerHtd.setName(Bytes.toBytes(peerTableName));
                    if (peerTableExists) {
                      if (createAgainIfAlreadyExists) {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Table " + peerTableName + " already present in cluster "
                              + peer + ". Going to drop it and create again");
                        }
                        disableTable(peerAdmin, peerTableName);
                        peerAdmin.deleteTable(peerTableName);
                        createTable(peer.getName(), peerAdmin, peerTableName, peerHtd, newSplitKeys);
                      } else {
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Table " + peerTableName + " already present in cluster "
                              + peer + ". Going to enable it");
                        }
                        enableTable(peerAdmin, peerTableName);
                      }
                    } else {
                      createTable(peer.getName(), peerAdmin, peerTableName, peerHtd, newSplitKeys);
                    }
                  } finally {
                    try {
                      peerAdmin.close();
                    } catch (IOException e) {
                      LOG.warn("Fail to close the HBaseAdmin of peers", e);
                    }
                  }
                }
              }
            }
          } finally {
            try {
              admin.close();
            } catch (IOException e) {
              LOG.warn("Fail to close the HBaseAdmin", e);
            }
          }
          return null;
        }
        private void createTable(final String clusterName, HBaseAdmin admin,
            String clusterTableName, HTableDescriptor htd, byte[][] newSplitKeys)
            throws IOException {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Creating table " + clusterTableName + " in the cluster " + clusterName);
          }
          admin.createTable(htd, newSplitKeys);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Created table " + clusterTableName + " in the cluster " + clusterName);
          }
        }
      }));
    }
  }
  

  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(String tableName) throws IOException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(byte[] tableName) throws IOException {
    final String tableNameAsString = Bytes.toString(tableName);
    // first check the global zk
    Map<String, String> clusterAddresses = null;
    boolean exists = false;
    try {
      exists = znodes.isTableStateExist(tableNameAsString);
      if (!exists) {
        return false;
      }
      clusterAddresses = znodes.listClusterAddresses();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    // access the cluster one by one
    List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
    for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
      results.add(pool.submit(new CrossSiteCallable<Boolean>(conf) {
        @Override
        public Boolean call() throws Exception {
          String clusterTableName = CrossSiteUtil.getClusterTableName(tableNameAsString,
              entry.getKey());
          HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue());
          try {
            return admin.tableExists(clusterTableName);
          } finally {
            try {
              admin.close();
            } catch (IOException e) {
              LOG.warn("Fail to close the HBaseAdmin", e);
            }
          }
        }
      }));
    }
    try {
      for (Future<Boolean> result : results) {
        if (!result.get()) {
          exists = false;
          break;
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to check whether the table exists:" + tableNameAsString, e);
      throw new IOException(e);
    }
    return exists;
  }

  /**
   * Method for getting the tableDescriptor
   * 
   * @param tableName
   *          as a byte []
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte[] tableName) throws IOException {
    return getTableDescriptor(Bytes.toString(tableName));
  }

  /**
   * Method for getting the tableDescriptor
   * 
   * @param tableName
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final String tableName) throws IOException {
    try {
      return znodes.getTableDesc(tableName);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Lists all of the names of cross site tables.
   * 
   * @return the list of table names
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public String[] getTableNames() throws IOException {
    try {
      return znodes.getTableNames();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Lists all of the names of cross site tables matching the given pattern
   * 
   * @param pattern
   *          The compiled regular expression to match against
   * @return the list of table names
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public String[] getTableNames(Pattern pattern) throws IOException {
    try {
      return znodes.getTableNames(pattern);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Lists all of the names of cross site tables matching the given regex
   * 
   * @param regex
   *          The regular expression to match against
   * @return the list of table names
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public String[] getTableNames(String regex) throws IOException {
    return getTableNames(Pattern.compile(regex));
  }

  /**
   * Adds a column to an existing cross site table. Asynchronous operation.
   * 
   * If this column is replica-enabled, add it to the peers.
   * 
   * @param tableName
   *          name of the table to add column to
   * @param column
   *          column descriptor of column to be added
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void addColumn(String tableName, HColumnDescriptor column) throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Adds a column to an existing cross site table. Asynchronous operation.
   * 
   * If this column is replica-enabled, add it to the peers.
   * 
   * @param tableName
   *          name of the table to add column to
   * @param column
   *          column descriptor of column to be added
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void addColumn(byte[] tableName, HColumnDescriptor column) throws IOException {
    try {
      addColumnInternal(Bytes.toString(tableName), column);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a column from a cross site table. Asynchronous operation.
   * 
   * @param tableName
   *          name of table
   * @param columnName
   *          name of column to be deleted
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void deleteColumn(final byte[] tableName,final String columnName) throws IOException {
    try {
      deleteColumnInternal(Bytes.toString(tableName), Bytes.toBytes(columnName));
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a column from a cross site table. Asynchronous operation.
   * 
   * @param tableName
   *          name of table
   * @param columnName
   *          name of column to be deleted
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void deleteColumn(String tableName, String columnName) throws IOException {
    deleteColumn(Bytes.toBytes(tableName), columnName);
  }

  /**
   * Modifies an existing column family of a cross site table. Asynchronous operation.
   * 
   * @param tableName
   *          name of table
   * @param column
   *          new column descriptor to use
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void modifyColumn(byte[] tableName, HColumnDescriptor column) throws IOException {
    try {
      modifyColumnInternal(Bytes.toString(tableName), column);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Modifies an existing column family on a cross site table. Asynchronous operation.
   * 
   * If the existing column is a replicated one, the modification is not allowed, and an exception
   * will be thrown.
   * 
   * @param tableName
   *          name of table
   * @param column
   *          new column descriptor to use
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void modifyColumn(String tableName, HColumnDescriptor column) throws IOException {
    modifyColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Modifies an existing column family of a cross site table. Asynchronous operation.
   * 
   * @param tableName
   *          name of table
   * @param htd
   *          new column descriptor to use
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void modifyTable(String tableName, HTableDescriptor htd) throws IOException {
    modifyTable(Bytes.toBytes(tableName), htd);
  }

  /**
   * Modifies an existing column family of a cross site table. Asynchronous operation.
   * 
   * @param tableName
   *          name of table
   * @param htd
   *          new column descriptor to use
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public void modifyTable(byte[] tableName, HTableDescriptor htd) throws IOException {
    try {
      modifyTableInternal(tableName, htd);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Modifies the column.
   * 
   * @param tableName
   * @param hcd
   * @throws IOException
   * @throws KeeperException
   */
  private void modifyColumnInternal(final String tableName, final HColumnDescriptor hcd)
      throws IOException, KeeperException {
    int tries = 0;
    for (; tries < numRetries * retryLongerMultiplier; ++tries) {
      if (znodes.lockTable(tableName)) {
        try {
          final HTableDescriptor htd = getTableDescriptor(tableName);
          final HTableDescriptor oldHtd = new HTableDescriptor(htd);
          final HColumnDescriptor oldHcd = htd.getFamily(hcd.getName());
          if (oldHcd == null) {
            throw new InvalidFamilyOperationException("Family '" + hcd.getNameAsString()
                + "' doesn't exists so cannot be modified");
          }
          htd.addFamily(hcd);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Modifying the column of the cross site table : " + tableName + " column : "
                + hcd);
          }
          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }
          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableName);
            } else if (TableState.MODIFYINGCOLUMN.equals(tableState)) {
              if (!htd.equals(znodes.getProposedTableDesc(tableName))) {
                throw new TableAbnormalStateException(
                    "A previous incomplete modifyColumn request with different HColumnDescriptor"
                        + " details! Please pass same details");
              }
              LOG.info("Try to modify a column for the cross site table " + tableName
                  + " in the MODIFYINGCOLUMN state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }

          Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
          znodes.writeProposedTableDesc(tableName, htd);
          // update the table state MODIFYINGCOLUMN
          znodes.setTableState(tableName, TableState.MODIFYINGCOLUMN);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusters.entrySet()) {
            final String clusterName = entry.getKey();
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
              @Override
              public Void call() throws Exception {
                ClusterInfo ci = entry.getValue();
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Modifying the column " + hcd.getNameAsString() + " of the table "
                      + clusterTableName + " in cluster " + clusterName);
                }
                HBaseAdmin admin = createHBaseAmin(configuration, ci.getAddress());
                try {
                  admin.modifyColumn(clusterTableName, hcd);
                } finally {
                  try {
                    admin.close();
                  } catch (IOException e) {
                    LOG.warn("Fail to close the HBaseAdmin", e);
                  }
                }
                // When this column was not replication enabled but changing the scope now, we
                // need to add the column into the peer table also
                // When this column was replication enabled but changing the scope to 0 now, no
                // need to delete the column. Let that be there in the peer with old data.
                if (hcd.getScope() > 0 && ci.getPeers() != null) {
                  HColumnDescriptor peerHcd = new HColumnDescriptor(hcd);
                  peerHcd.setScope(0);
                  boolean tableAlreadyReplicated = isReplicatedTable(oldHtd);
                  for (ClusterInfo peer : ci.getPeers()) {
                    if (oldHcd.getScope() == 0) {
                      if (tableAlreadyReplicated) {
                        addColumn(configuration, clusterName, peer.getAddress(), tableName,
                            peerHcd, true);
                      } else {
                        String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                            clusterName, peer.getName());
                        HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                        try {
                          if (peerAdmin.tableExists(peerTableName)) {
                            addColumn(peerAdmin, clusterName, peer.getAddress(), tableName,
                                peerHcd, true);
                          } else {
                            // create the table in the peer.
                            byte[][] splitKeys = getTableSplitsForCluster(tableName, clusterName);
                            HTableDescriptor peerHtd = new HTableDescriptor(htd);
                            peerHtd.setName(Bytes.toBytes(peerTableName));
                            for (HColumnDescriptor column : peerHtd.getColumnFamilies()) {
                              // only create the CFs that have the scope as 1.
                              if (column.getScope() > 0) {
                                column.setScope(0);
                              } else {
                                peerHtd.removeFamily(column.getName());
                              }
                            }
                            if (LOG.isDebugEnabled()) {
                              LOG.debug("Creating table " + peerTableName + " in peer cluster "
                                  + peer);
                            }
                            peerAdmin.createTable(peerHtd, splitKeys);
                          }
                        } finally {
                          try {
                            peerAdmin.close();
                          } catch (IOException e) {
                            LOG.warn("Fail to close the HBaseAdmin", e);
                          }
                        }
                      }
                    } else {
                      modifyColumnInPeer(configuration, clusterName, tableName, peer, peerHcd);
                    }
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            // modify the znodes to the {tableName}.
            znodes.modifyTableDesc(tableName, htd);
            znodes.setTableState(tableName, TableState.DISABLED);
            znodes.deleteProposedTableDesc(tableName);
          } catch (Exception e) {
            LOG.error("Fail to modify the column of the table " + tableName, e);
            throw new IOException(e);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("The column of the cross site table " + tableName + " is modified");
          }
          return;
        } finally {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < numRetries * retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable enable");
        }
      }
    }
    // All retries for acquiring locks failed! Thowing Exception
    throw new RetriesExhaustedException("Not able to acquire table lock after " + tries + " tries");
  }

  private static void modifyColumnInPeer(Configuration conf, String masterClusterName,
      String tableName, ClusterInfo peer, HColumnDescriptor peerHcd) throws IOException {
    HBaseAdmin peerAdmin = createHBaseAmin(conf, peer.getAddress());
    String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName, masterClusterName,
        peer.getName());
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disabling table " + peerTableName + " in peer cluster " + peer
            + " as part of modifying column " + peerHcd.getNameAsString());
      }
      disableTable(peerAdmin, peerTableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Modifying column " + peerHcd.getNameAsString() + " in table " + peerTableName
            + " in peer cluster " + peer);
      }
      peerAdmin.modifyColumn(peerTableName, peerHcd);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Enabling table " + peerTableName + " in peer cluster " + peer
            + " after modifying column " + peerHcd.getNameAsString());
      }
      enableTable(peerAdmin, peerTableName);
    } finally {
      try {
        peerAdmin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the HBaseAdmin", e);
      }
    }
  }

  private byte[][] getTableSplitsForCluster(String tableName, String clusterName)
      throws KeeperException, IOException {
    byte[][] splitKeys = znodes.getTableSplitKeys(tableName);
    if (splitKeys != null) {
      ClusterLocator clusterLocator = znodes.getClusterLocator(tableName);
      splitKeys = clusterLocator.getSplitKeys(clusterName, splitKeys);
    }
    return splitKeys;
  }

  /**
   * Checks whether the table is available.
   * 
   * @param tableName
   * @param perClusterCheck
   * @throws IOException
   * @throws KeeperException
   */
  private boolean isTableAvailableInternal(final String tableName, boolean perClusterCheck)
      throws IOException, KeeperException {
    boolean available = znodes.isTableStateExist(tableName);
    if (!available)
      return available;
    if (perClusterCheck) {
      Map<String, String> clusterAddresses = znodes.listClusterAddresses();
      // access the cluster one by one
      List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
      for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
        results.add(pool.submit(new CrossSiteCallable<Boolean>(conf) {
          @Override
          public Boolean call() throws Exception {
            String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, entry.getKey());
            HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue());
            try {
              return admin.isTableAvailable(clusterTableName);
            } finally {
              try {
                admin.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin", e);
              }
            }
          }
        }));
      }
      try {
        for (Future<Boolean> result : results) {
          available = result.get();
          if (!available) {
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Fail to check whether the table is available:" + tableName, e);
        throw new IOException(e);
      }
    }
    return available;
  }

  /**
   * Checks whether the table is enabled.
   * 
   * @param tableName
   * @param perClusterCheck
   * @throws IOException
   * @throws KeeperException
   */
  private boolean isTableEnabledInternal(final String tableName, boolean perClusterCheck)
      throws IOException, KeeperException {
    // firstly check the global zk
    TableState tableState = znodes.getTableState(tableName);
    boolean enabled = TableState.ENABLED.equals(tableState);
    if (!enabled) {
      return false;
    }
    if (perClusterCheck) {
      Map<String, String> clusterAddresses = znodes.listClusterAddresses();
      // access the cluster one by one
      List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
      for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
        results.add(pool.submit(new CrossSiteCallable<Boolean>(conf) {
          @Override
          public Boolean call() throws Exception {
            String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, entry.getKey());
            HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue());
            try {
              return admin.isTableEnabled(clusterTableName);
            } finally {
              try {
                admin.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin", e);
              }
            }
          }
        }));
      }
      try {
        for (Future<Boolean> result : results) {
          enabled = result.get();
          if (!enabled) {
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Fail to check whether the table is enabled:" + tableName, e);
        throw new IOException(e);
      }
    }
    return enabled;
  }

  private static HBaseAdmin createHBaseAmin(Configuration baseConf, String clusterAddress)
      throws IOException {
    Configuration clusterConf = new Configuration(baseConf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    return new HBaseAdmin(clusterConf);
  }

  /**
   * Checks whether the table is disabled.
   * 
   * @param tableName
   * @param perClusterCheck
   * @throws IOException
   * @throws KeeperException
   */
  private boolean isTableDisabledInternal(final String tableName, boolean perClusterCheck)
      throws IOException, KeeperException {
    // firstly check the global zk
    TableState tableState = znodes.getTableState(tableName);
    boolean disabled = TableState.DISABLED.equals(tableState);
    if (!disabled) {
      return false;
    }
    if (perClusterCheck) {
      Map<String, String> clusterAddresses = znodes.listClusterAddresses();
      // access the cluster one by one
      List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
      for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
        results.add(pool.submit(new CrossSiteCallable<Boolean>(conf) {
          @Override
          public Boolean call() throws Exception {
            String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, entry.getKey());
            HBaseAdmin admin = createHBaseAmin(configuration, entry.getValue());
            try {
              return admin.isTableDisabled(clusterTableName);
            } finally {
              try {
                admin.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin", e);
              }
            }
          }
        }));
      }
      try {
        for (Future<Boolean> result : results) {
          disabled = result.get();
          if (!disabled) {
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Fail to check whether the table is disabled:" + tableName, e);
        throw new IOException(e);
      }
    }
    return disabled;
  }

  /**
   * Deletes the column.
   * 
   * @param tableName
   * @param columnName
   * @throws IOException
   * @throws KeeperException
   */
  private void deleteColumnInternal(final String tableName, final byte[] columnName)
      throws IOException, KeeperException {
    int tries = 0;
    for (; tries < numRetries * retryLongerMultiplier; ++tries) {
      if (znodes.lockTable(tableName)) {
        try {
          HTableDescriptor htd = getTableDescriptor(tableName);
          if (!htd.hasFamily(columnName)) {
            throw new InvalidFamilyOperationException("Column family '"
                + Bytes.toString(columnName) + "' does not exist");
          }
          final HColumnDescriptor removedHcd = htd.removeFamily(columnName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting column from the cross site table " + tableName);
          }
          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }
          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableName);
            } else if (TableState.DELETINGCOLUMN.equals(tableState)) {
              if (!htd.equals(znodes.getProposedTableDesc(tableName))) {
                throw new TableAbnormalStateException(
                    "A previous incomplete deleteColumn request with different HColumnDescriptor"
                        + " details! Please pass same details");
              }
              LOG.info("Try to delete a column for the cross site table " + tableName
                  + " in the DELETINGCOLUMN state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }
          Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
          znodes.writeProposedTableDesc(tableName, htd);
          // update the table state to DELETINGCOLUMN
          znodes.setTableState(tableName, TableState.DELETINGCOLUMN);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusters.entrySet()) {
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
              @Override
              public Void call() throws Exception {
                ClusterInfo ci = entry.getValue();
                String masterClusterName = entry.getKey();
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableName,
                    masterClusterName);
                deleteColumn(configuration, ci.getAddress(), clusterTableName, columnName, false);
                if (removedHcd.getScope() > 0) {
                  // This removed column was replication enabled! So deleting from peer tables as
                  // well
                  Set<ClusterInfo> peers = ci.getPeers();
                  if (peers != null) {
                    for (ClusterInfo peer : peers) {
                      String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                          masterClusterName, peer.getName());
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Deleting column " + Bytes.toString(columnName) + " from table "
                            + peerTableName + " in peer cluster " + peer);
                      }
                      deleteColumn(configuration, peer.getAddress(), peerTableName, columnName, true);
                    }
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            // modify the znodes to the {tableName}.
            znodes.modifyTableDesc(tableName, htd);
            znodes.setTableState(tableName, TableState.DISABLED);
            znodes.deleteProposedTableDesc(tableName);
          } catch (Exception e) {
            LOG.error("Fail to delete the column from the table " + tableName, e);
            throw new IOException(e);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("The column of the cross site table " + tableName + " is deleted");
          }
          return;
        } finally {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < numRetries * retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable enable");
        }
      }
    }
  }

  private static void deleteColumn(Configuration conf, String clusterAddress,
      String clusterTableName, byte[] columnName, boolean peerCluster) throws IOException {
    HBaseAdmin admin = createHBaseAmin(conf, clusterAddress);
    try {
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Disabling " + clusterTableName + " in peer cluster as part of adding column");
        }
        disableTable(admin, clusterTableName);
      }
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(clusterTableName));
      if (htd.hasFamily(columnName)) {
        admin.deleteColumn(TableName.valueOf(clusterTableName), columnName);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The column " + Bytes.toString(columnName) + " has been deleted from table "
              + clusterTableName);
        }
      }
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Enabling " + clusterTableName + " in peer cluster after adding column");
        }
        enableTable(admin, clusterTableName);
      }
    } catch (InvalidFamilyOperationException e) {
      // Supress InvalidFamilyOperationException.
      LOG.debug(e);
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the HBaseAdmin", e);
      }
    }
  }

  /**
   * Adds a column.
   * 
   * @param tableName
   * @param hcd
   * @throws IOException
   * @throws KeeperException
   */
  private void addColumnInternal(final String tableName, final HColumnDescriptor hcd)
      throws IOException, KeeperException {
    int tries = 0;
    for (; tries < numRetries * retryLongerMultiplier; ++tries) {
      if (znodes.lockTable(tableName)) {
        try {
          final HTableDescriptor htd = getTableDescriptor(tableName);
          if (htd.hasFamily(hcd.getName())) {
            throw new InvalidFamilyOperationException("Family '" + hcd.getNameAsString()
                + "' already exists so cannot be added");
          }
          final boolean tableAlreadyReplicated = isReplicatedTable(htd);
          htd.addFamily(hcd);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding column " + hcd + " to the cross site table " + tableName);
          }

          TableState tableState = znodes.getTableState(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState);
          }
          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableName);
            } else if (TableState.ADDINGCOLUMN.equals(tableState)) {
              if (!htd.equals(znodes.getProposedTableDesc(tableName))) {
                throw new TableAbnormalStateException(
                    "A previous incomplete addColumn request with different HColumnDescriptor"
                        + " details! Please pass same details");
              }
              LOG.info("Try to add a column for the cross site table " + tableName
                  + " in the ADDINGCOLUMN state");
            } else {
              throw new TableAbnormalStateException(tableName + ":" + tableState);
            }
          }

          Map<String, ClusterInfo> clusters = znodes.listClusterInfos();
          znodes.writeProposedTableDesc(tableName, htd);
          // update the state to ADDINGCOLUMN
          znodes.setTableState(tableName, TableState.ADDINGCOLUMN);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusters.entrySet()) {
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
              @Override
              public Void call() throws Exception {
                ClusterInfo ci = entry.getValue();
                String mainClusterName = entry.getKey();
                addColumn(configuration, mainClusterName, ci.getAddress(), tableName, hcd, false);
                // creates the table in peers.
                if (hcd.getScope() > 0) {
                  HColumnDescriptor peerHCD = new HColumnDescriptor(hcd);
                  peerHCD.setScope(0);
                  if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                    for (ClusterInfo peer : ci.getPeers()) {
                      String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                          mainClusterName, peer.getName());
                      if (tableAlreadyReplicated) {
                        // Already this table is present in the peer. Just need to add the new
                        // column to the table
                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Adding column " + hcd + " to table " + peerTableName
                              + " in the peer " + peer);
                        }
                        addColumn(configuration, mainClusterName, peer.getAddress(), tableName,
                            peerHCD, true);
                      } else {
                        HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                        try {
                          if (peerAdmin.tableExists(peerTableName)) {
                            // Already this table is present in the peer. Just need to add the new
                            // column to the table
                            if (LOG.isDebugEnabled()) {
                              LOG.debug("Adding column " + hcd + " to table " + peerTableName
                                  + " in the peer " + peer);
                            }
                            addColumn(configuration, mainClusterName, peer.getAddress(), tableName,
                                peerHCD, true);
                          } else {
                            // Till now there were no cfs in the table for replication. So tables
                            // are
                            // not yet there in peer. Create it with just one column (ie. this
                            // column)
                            if (LOG.isDebugEnabled()) {
                              LOG.debug("Creating table " + peerTableName + " in peer cluster "
                                  + peer + " as newly added column " + hcd + " is replicatable");
                            }
                            byte[][] splitKeys = getTableSplitsForCluster(tableName, entry.getKey());
                            HTableDescriptor peerHtd = new HTableDescriptor(htd);
                            for (HColumnDescriptor hcd : peerHtd.getColumnFamilies()) {
                              // only create the CFs that have the scope as 1.
                              if (hcd.getScope() > 0) {
                                hcd.setScope(0);
                              } else {
                                peerHtd.removeFamily(hcd.getName());
                              }
                            }
                            peerHtd.setName(Bytes.toBytes(peerTableName));
                            peerAdmin.createTable(peerHtd, splitKeys);
                          }
                        } finally {
                          try {
                            peerAdmin.close();
                          } catch (IOException e) {
                            LOG.warn("Fail to close the HBaseAdmin of peers", e);
                          }
                        }
                      }
                    }
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            // modify the znodes to the {tableName}.
            znodes.modifyTableDesc(tableName, htd);
            znodes.setTableState(tableName, TableState.DISABLED);
            znodes.deleteProposedTableDesc(tableName);
          } catch (Exception e) {
            LOG.error("Fail to add a column to the table " + tableName, e);
            throw new IOException(e);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("The column of the cross site table " + tableName + " is added");
          }
          return;
        } finally {
          znodes.unlockTable(tableName);
        }
      }
      if (tries < numRetries * retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable enable");
        }
      }
    }
    // All retries for acquiring locks failed! Thowing Exception
    throw new RetriesExhaustedException("Not able to acquire table lock after " + tries + " tries");
  }

  private static void addColumn(HBaseAdmin admin, String clusterName, String clusterAddress,
      String tableName, HColumnDescriptor hcd, boolean peerCluster) throws IOException {
    String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
    try {
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Disabling " + clusterTableName + " in peer cluster " + clusterName + " : "
              + clusterAddress + " as part of adding column");
        }
        disableTable(admin, clusterTableName);
      }
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(clusterTableName));
      if (htd.hasFamily(hcd.getName())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The column " + Bytes.toString(hcd.getName()) + " has been existent in table "
              + clusterTableName);
        }
      } else {
        admin.addColumn(Bytes.toBytes(clusterTableName), hcd);
      }
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Enabling " + clusterTableName + " in peer cluster " + clusterName + " : "
              + clusterAddress + " after adding column");
        }
        enableTable(admin, clusterTableName);
      }
    } catch (InvalidFamilyOperationException e) {
      // Supress InvalidFamilyOperationException.
      LOG.debug(e);
    }
  }

  private static void addColumn(Configuration conf, String clusterName, String clusterAddress,
      String tableName, HColumnDescriptor hcd, boolean peerCluster) throws IOException {
    String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
    HBaseAdmin admin = createHBaseAmin(conf, clusterAddress);
    try {
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Disabling " + clusterTableName + " in peer cluster " + clusterName + " : "
              + clusterAddress + " as part of adding column");
        }
        disableTable(admin, clusterTableName);
      }
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(clusterTableName));
      if (htd.hasFamily(hcd.getName())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The column " + Bytes.toString(hcd.getName()) + " has been existent in table "
              + clusterTableName);
        }
      } else {
        admin.addColumn(Bytes.toBytes(clusterTableName), hcd);
      }
      if (peerCluster) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Enabling " + clusterTableName + " in peer cluster " + clusterName + " : "
              + clusterAddress + " after adding column");
        }
        enableTable(admin, clusterTableName);
      }
    } catch (InvalidFamilyOperationException e) {
      // Supress InvalidFamilyOperationException.
      LOG.debug(e);
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the HBaseAdmin", e);
      }
    }
  }

  /**
   * Modifies the table.
   * 
   * @param tableName
   * @param htd
   * @throws IOException
   * @throws KeeperException
   */
  private void modifyTableInternal(final byte[] tableName, final HTableDescriptor htd)
      throws IOException, KeeperException {
    final String tableNameAsString = Bytes.toString(tableName);
    int tries = 0;
    for (; tries < numRetries * retryLongerMultiplier; ++tries) {
      if (znodes.lockTable(tableNameAsString)) {
        try {
          final HTableDescriptor oldHtd = getTableDescriptor(tableName);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Start to modify the cross site table " + tableNameAsString);
          }
          TableState tableState = znodes.getTableState(tableNameAsString);
          if (LOG.isDebugEnabled()) {
            LOG.debug("The state of " + tableName + " is " + tableState.toString());
          }
          if (!TableState.DISABLED.equals(tableState)) {
            if (TableState.ENABLED.equals(tableState)) {
              throw new TableNotDisabledException(tableNameAsString);
            } else if (TableState.MODIFYING.equals(tableState)) {
              if (!htd.equals(znodes.getProposedTableDesc(tableNameAsString))) {
                throw new TableAbnormalStateException(
                    "A previous incomplete modifyTable request with different HColumnDescriptor"
                        + " details! Please pass same details");
              }
              LOG.info("Try to modify the cross site table " + tableName
                  + " in the MODIFYING state");
            } else {
              throw new TableAbnormalStateException(tableNameAsString + ":" + tableState.toString());
            }
          }

          Map<String, ClusterInfo> clusterInfos = znodes.listClusterInfos();
          znodes.writeProposedTableDesc(tableNameAsString, htd);
          // update the table state to MODIFYING
          znodes.setTableState(tableNameAsString, TableState.MODIFYING);
          // access the cluster one by one
          List<Future<Void>> results = new ArrayList<Future<Void>>();
          for (final Entry<String, ClusterInfo> entry : clusterInfos.entrySet()) {
            results.add(pool.submit(new CrossSiteCallable<Void>(conf) {
              @Override
              public Void call() throws Exception {
                ClusterInfo ci = entry.getValue();
                String clusterTableName = CrossSiteUtil.getClusterTableName(tableNameAsString,
                    entry.getKey());
                HTableDescriptor newHtd = new HTableDescriptor(htd);
                newHtd.setName(Bytes.toBytes(clusterTableName));
                HBaseAdmin admin = createHBaseAmin(configuration, ci.getAddress());
                try {
                  admin.modifyTable(Bytes.toBytes(clusterTableName), newHtd);
                } finally {
                  try {
                    admin.close();
                  } catch (IOException e) {
                    LOG.warn("Fail to close the HBaseAdmin", e);
                  }
                }
                if (isReplicatedTable(oldHtd)) {
                  if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                    HTableDescriptor peerHtd = new HTableDescriptor(htd);
                    for (HColumnDescriptor hcd : peerHtd.getColumnFamilies()) {
                      if (hcd.getScope() > 0) {
                        hcd.setScope(0);
                      } else {
                        peerHtd.removeFamily(hcd.getName());
                      }
                    }
                    for (ClusterInfo peer : ci.getPeers()) {
                      String peerTableName = CrossSiteUtil.getPeerClusterTableName(
                          tableNameAsString, entry.getKey(), peer.getName());
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating the table " + peerTableName + " to the peer "
                            + peer.getAddress());
                      }
                      HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                      try {
                        peerHtd.setName(Bytes.toBytes(peerTableName));
                        disableTable(peerAdmin, peerTableName);
                        peerAdmin.modifyTable(Bytes.toBytes(peerTableName), peerHtd);
                        enableTable(peerAdmin, peerTableName);
                      } finally {
                        try {
                          peerAdmin.close();
                        } catch (IOException e) {
                          LOG.warn("Fail to close the HBaseAdmin of peers", e);
                        }
                      }
                    }
                  }
                } else if (isReplicatedTable(newHtd)) {
                  if (ci.getPeers() != null && !ci.getPeers().isEmpty()) {
                    HTableDescriptor peerHtd = new HTableDescriptor(htd);
                    for (HColumnDescriptor hcd : peerHtd.getColumnFamilies()) {
                      if (hcd.getScope() > 0) {
                        hcd.setScope(0);
                      } else {
                        peerHtd.removeFamily(hcd.getName());
                      }
                    }
                    byte[][] splitKeys = getTableSplitsForCluster(tableNameAsString, entry.getKey());
                    for (ClusterInfo peer : ci.getPeers()) {
                      String peerTableName = CrossSiteUtil.getPeerClusterTableName(
                          tableNameAsString, entry.getKey(), peer.getName());
                      peerHtd.setName(Bytes.toBytes(peerTableName));
                      HBaseAdmin peerAdmin = createHBaseAmin(configuration, peer.getAddress());
                      try {
                        if (!peerAdmin.tableExists(peerTableName)) {
                          if (LOG.isDebugEnabled()) {
                            LOG.debug("Creating table " + peerTableName + " in peer cluster "
                                + peer + " as the modified table " + newHtd + " is replicatable");
                          }
                          peerAdmin.createTable(peerHtd, splitKeys);
                        } else {
                          disableTable(peerAdmin, peerTableName);
                          peerAdmin.modifyTable(Bytes.toBytes(peerTableName), peerHtd);
                          enableTable(peerAdmin, peerTableName);
                        }
                      } finally {
                        try {
                          peerAdmin.close();
                        } catch (IOException e) {
                          LOG.warn("Fail to close the HBaseAdmin of peers", e);
                        }
                      }
                    }
                  }
                }
                return null;
              }
            }));
          }
          try {
            for (Future<Void> result : results) {
              result.get();
            }
            // modify the znodes to the {tableName}.
            znodes.modifyTableDesc(tableNameAsString, htd);
            znodes.setTableState(tableNameAsString, TableState.DISABLED);
            znodes.deleteProposedTableDesc(tableNameAsString);
          } catch (Exception e) {
            LOG.error("Fail to modify the table " + tableNameAsString, e);
            throw new IOException(e);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("The cross site table " + tableNameAsString + " is modified");
          }
          return;
        } finally {
          znodes.unlockTable(tableNameAsString);
        }
      }
      if (tries < numRetries * retryLongerMultiplier - 1) {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for cross site HTable enable");
        }
      }
    }
    // All retries for acquiring locks failed! Thowing Exception
    throw new RetriesExhaustedException("Not able to acquire table lock after " + tries + " tries");
  }

  private static boolean isReplicatedTable(HTableDescriptor htd) {
    boolean replicationEnabled = false;
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      replicationEnabled = (hcd.getScope() > 0);
      if (replicationEnabled) {
        break;
      }
    }
    return replicationEnabled;
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  public void close() throws IOException {
    if (pool != null) {
      pool.shutdown();
    }
    if (zkw != null) {
      zkw.close();
    }
  }

  /**
   * Add the znodes for the descriptor, split keys and locator to the table znode.
   * 
   * @param desc
   * @param splitKeys
   * @param locator
   * @throws KeeperException
   * @throws IOException
   */
  private void addTableChildrenZNodes(HTableDescriptor desc, byte[][] splitKeys,
      ClusterLocator locator) throws KeeperException, IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      String tableName = desc.getNameAsString();
      DataOutput out = new DataOutputStream(stream);
      desc.write(out);
      ZKUtil.createSetData(this.zkw, znodes.getTableDescZNode(tableName), stream.toByteArray());
      stream.close();
      stream = new ByteArrayOutputStream();
      out = new DataOutputStream(stream);
      CrossSiteUtil.writeSplitKeys(splitKeys, out);
      ZKUtil
          .createSetData(this.zkw, znodes.getTableSplitKeysZNode(tableName), stream.toByteArray());
      stream.close();
      stream = new ByteArrayOutputStream();
      out = new DataOutputStream(stream);
      ClusterLocatorRPCObject locatorRPCObj = new ClusterLocatorRPCObject(locator);
      locatorRPCObj.write(out);
      ZKUtil
          .createSetData(this.zkw, znodes.getClusterLocatorZNode(tableName), stream.toByteArray());
      stream.close();
      ZKUtil.createSetData(this.zkw, znodes.getTableStateZNode(tableName),
          Bytes.toBytes(TableState.ENABLED.toString()));
    } finally {
      stream.close();
    }
  }

  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }

  public boolean isAborted() {
    return this.aborted;
  }

  /**
   * Compacts a table or an individual region for a cross site big table.
   * Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   * @throws IOException
   * @throws InterruptedException
   */
  public void compact(String tableNameOrRegionName) throws IOException, InterruptedException {
    compact(tableNameOrRegionName, null);
  }

  /**
   * Compacts a table or an individual region for a cross site big table.
   * Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   * @throws IOException
   * @throws InterruptedException
   */
  public void compact(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    compact(Bytes.toString(tableNameOrRegionName), null);
  }

  /**
   * Compacts a column family within a table or an individual region for a cross site big table.
   * Asynchronous operation.
   * 
   * @param tableOrRegionName
   * @param columnFamily
   * @throws IOException
   * @throws InterruptedException
   */
  public void compact(String tableOrRegionName, String columnFamily) throws IOException,
      InterruptedException {
    compactInternal(tableOrRegionName, columnFamily, false);
  }

  /**
   * Compacts a column family within a table or an individual region for a cross site big table.
   * Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   * @param columnFamily
   * @throws IOException
   * @throws InterruptedException
   */
  public void compact(byte[] tableNameOrRegionName, byte[] columnFamily) throws IOException,
      InterruptedException {
    compactInternal(Bytes.toString(tableNameOrRegionName), Bytes.toString(columnFamily), false);
  }

  /**
   * Major compacts a table or an individual region. Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   *          table or region to major compact
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(String tableNameOrRegionName) throws IOException, InterruptedException {
    majorCompact(tableNameOrRegionName, null);
  }

  /**
   * Major compacts a table or an individual region. Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   *          table or region to major compact
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    majorCompact(tableNameOrRegionName, null);
  }

  /**
   * Major compacts a column family within a table or region. Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   *          table or region to major compact
   * @param columnFamily
   *          column family within a table or region
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(String tableNameOrRegionName, String columnFamily) throws IOException,
      InterruptedException {
    compactInternal(tableNameOrRegionName, columnFamily, true);
  }

  /**
   * Major compacts a column family within a table or region. Asynchronous operation.
   * 
   * @param tableNameOrRegionName
   *          table or region to major compact
   * @param columnFamily
   *          column family within a table or region
   * @throws IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(byte[] tableNameOrRegionName, byte[] columnFamily) throws IOException,
      InterruptedException {
    compactInternal(Bytes.toString(tableNameOrRegionName), Bytes.toString(columnFamily), true);
  }

  /**
   * Compacts the table or regions for the cross site big table.
   * 
   * @param tableOrRegionName
   * @param columnFamily
   * @param major
   * @throws IOException
   * @throws InterruptedException
   */
  private void compactInternal(final String tableOrRegionName, final String columnFamily,
      final boolean major)
      throws IOException, InterruptedException {
    try {
      String clusterName = null;
      String tableName = null;
      // Do table existence check
      TableState state = znodes.getTableStateAllowNull(tableOrRegionName);
      if (state == null) {
        // it's a region name or the argement is incorrect.
        String[] regionSplits = tableOrRegionName.split(",");
        if (regionSplits != null && regionSplits.length > 0) {
          tableName = regionSplits[0];
          clusterName = CrossSiteUtil.getClusterName(tableName);
        } else {
          throw new IOException("The table or region " + tableOrRegionName + " is not found");
        }
      }
      if (clusterName != null) {
        String clusterAddress = znodes.getClusterAddress(clusterName);
        if (clusterAddress != null) {
          Configuration clusterConf = new Configuration(conf);
          ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
          HBaseAdmin admin = new HBaseAdmin(clusterConf);
          try {
            if (major) {
              if (columnFamily != null) {
                admin.majorCompact(tableOrRegionName, columnFamily);
              } else {
                admin.majorCompact(tableOrRegionName);
              }
            } else {
              if (columnFamily != null) {
                admin.compact(tableOrRegionName, columnFamily);
              } else {
                admin.compact(tableOrRegionName);
              }
            }
          } finally {
            try {
              admin.close();
            } catch (IOException e) {
              LOG.warn("Fail to close the HBaseAdmin.", e);
            }
          }
        } else {
          throw new IOException("The table or region " + tableOrRegionName + " is not found");
        }
      } else if (state != null) {
        // call HBaseAdmin.compact for all underlying tables
        List<Throwable> exceptions = new ArrayList<Throwable>();
        List<Row> actions = new ArrayList<Row>();
        List<String> addresses = new ArrayList<String>();
        Map<String, String> clusterAddresses = znodes.listClusterAddresses();
        // access the cluster one by one
        Map<String, Future<Void>> results = new HashMap<String, Future<Void>>();
        for (final Entry<String, String> entry : clusterAddresses.entrySet()) {
          results.put(entry.getValue(), pool.submit(new CrossSiteCallable<Void>(conf) {

            @Override
            public Void call() throws Exception {
              Configuration clusterConf = new Configuration(configuration);
              ZKUtil.applyClusterKeyToConf(clusterConf, entry.getValue());
              String clusterTableName = CrossSiteUtil.getClusterTableName(tableOrRegionName,
                  entry.getKey());
              HBaseAdmin admin = new HBaseAdmin(clusterConf);
              try {
                if (major) {
                  if (columnFamily != null) {
                    admin.majorCompact(clusterTableName, columnFamily);
                  } else {
                    admin.majorCompact(clusterTableName);
                  }
                } else {
                  if (columnFamily != null) {
                    admin.compact(clusterTableName, columnFamily);
                  } else {
                    admin.compact(clusterTableName);
                  }
                }
              } finally {
                try {
                  admin.close();
                } catch (IOException e) {
                  LOG.warn("Fail to close the HBaseAdmin.", e);
                }
              }
              return null;
            }
          }));
        }
        for (Entry<String, Future<Void>> result : results.entrySet()) {
          try {
            result.getValue().get();
          } catch (ExecutionException e) {
            LOG.warn("Fail to compact the table or region ", e);
            exceptions.add(e);
            // dummy actions
            actions.add(new DummyRow());
            addresses.add(result.getKey());
          }
        }
        if (exceptions.size() > 0) {
          throw new RetriesExhaustedWithDetailsException(exceptions, actions, addresses);
        }
      } else {
        throw new IOException("The table or region " + tableOrRegionName + " is not found");
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * A dummy row
   *
   */
  private static class DummyRow implements Row {

    @Override
    public int compareTo(Row o) {
      return 0;
    }

    @Override
    public byte[] getRow() {
      return HConstants.EMPTY_START_ROW;
    }
    
  }

  /********************* Unsupported APIs *********************/
  public void unassign(byte[] regionName, boolean force) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void closeRegion(String regionName, String serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void closeRegion(byte[] regionName, String serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public boolean closeRegionWithEncodedRegionName(String encodedRegionName, String serverName)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void closeRegion(ServerName sn, HRegionInfo hri) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void cloneSnapshot(byte[] snapshotName, byte[] tableName) throws IOException,
      TableExistsException, RestoreSnapshotException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void cloneSnapshot(String snapshotName, String tableName) throws IOException,
      TableExistsException, RestoreSnapshotException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HConnection getConnection() {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void move(byte[] encodedRegionName, byte[] destServerName) throws UnknownRegionException,
      MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public boolean setBalancerRunning(boolean on, boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public List<HRegionInfo> getTableRegions(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public synchronized byte[][] rollHLogWriter(String serverName) throws IOException,
      FailedLogCloseException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public String[] getMasterCoprocessors() {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public CompactionState getCompactionState(String tableNameOrRegionName) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public CompactionState getCompactionState(byte[] tableNameOrRegionName) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void snapshot(String snapshotName, String tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void snapshot(byte[] snapshotName, byte[] tableName) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void snapshot(String snapshotName, String tableName, Type type) throws IOException,
      SnapshotCreationException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void snapshot(SnapshotDescription snapshot) throws IOException, SnapshotCreationException,
      IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public MasterProtos.SnapshotResponse takeSnapshotAsync(SnapshotDescription snapshot) throws IOException,
      SnapshotCreationException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public boolean isSnapshotFinished(SnapshotDescription snapshot) throws IOException,
      HBaseSnapshotException, UnknownSnapshotException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void restoreSnapshot(byte[] snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void restoreSnapshot(String snapshotName) throws IOException, RestoreSnapshotException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public List<SnapshotDescription> listSnapshots() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void deleteSnapshot(byte[] snapshotName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void deleteSnapshot(String snapshotName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void assign(final byte[] regionName) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public boolean balancer() throws MasterNotRunningException, ZooKeeperConnectionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void split(final String tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void split(final byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void split(final String tableNameOrRegionName, final String splitPoint)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void split(final byte[] tableNameOrRegionName, final byte[] splitPoint)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public synchronized void shutdown() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public synchronized void stopMaster() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public synchronized void stopRegionServer(final String hostnamePort) {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void disableTableAsync(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void disableTableAsync(String tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void enableTableAsync(byte[] tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public void enableTableAsync(String tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported.
   */
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    throw new UnsupportedOperationException();
  }
}
// TODO cluster level lock for some ops?
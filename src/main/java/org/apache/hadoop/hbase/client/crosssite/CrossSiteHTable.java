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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.crosssite.CrossSiteDummyAbortable;
import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes.TableState;
import org.apache.hadoop.hbase.crosssite.TableAbnormalStateException;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator.RowNotLocatableException;
import org.apache.hadoop.hbase.crosssite.locator.PrefixClusterLocator;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * <p>
 * Used to communicate with a cross site table.
 * 
 * <p>
 * This class is not thread safe for reads nor write.
 * 
 * <p>
 * Note that this class implements the {@link Closeable} interface. When a CrossSiteHTable instance
 * is no longer required, it *should* be closed in order to ensure that the underlying resources are
 * promptly released. Please note that the close method can throw java.io.IOException that must be
 * handled.
 * 
 * @see CrossSiteHBaseAdmin for create, drop, list, enable and disable of tables.
 */
public class CrossSiteHTable extends HTable implements CrossSiteHTableInterface {
  private static final Log LOG = LogFactory.getLog(CrossSiteHTable.class);

  protected volatile Configuration configuration;
  protected final byte[] tableName;
  protected String tableNameAsString;
  protected boolean autoFlush;
  protected boolean clearBufferOnFail;
  protected int maxKeyValueSize;
  protected int scannerCaching;
  protected boolean closed = false;
  protected boolean cleanupPoolOnClose;
  protected long writeBufferSize;
  protected int operationTimeout = -1; // default value is -1.

  private ZooKeeperWatcher zkw;
  protected CrossSiteZNodes znodes;

  protected Map<String, HTableInterface> tablesCache;
  protected boolean failover;
  protected CachedZookeeperInfo cachedZKInfo;

  protected final ExecutorService pool; // used to dispatch the execution to each clusters.
  private final HTableInterfaceFactory hTableFactory;

  public CrossSiteHTable(Configuration conf, final String tableName) throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }

  public CrossSiteHTable(Configuration conf, final byte[] tableName) throws IOException {
    this(conf, tableName, null);
  }

  public CrossSiteHTable(Configuration conf, final byte[] tableName, final ExecutorService pool)
      throws IOException {
    this(conf, tableName, pool, new HTableFactory());
  }

  public CrossSiteHTable(Configuration conf, final byte[] tableName, final ExecutorService pool,
      HTableInterfaceFactory hTableFactory) throws IOException {
    this.configuration = getCrossSiteConf(conf, conf.get(CrossSiteConstants.CROSS_SITE_ZOOKEEPER));
    this.tableName = tableName;
    if (pool != null) {
      this.cleanupPoolOnClose = false;
      this.pool = pool;
    } else {
      this.cleanupPoolOnClose = true;
      this.pool = getDefaultExecutor(this.configuration);
    }
    this.hTableFactory = hTableFactory;
    try {
      finishSetup();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ExecutorService getPool() {
    return pool;
  }

  /**
   * Gets the configuration with the given cluster key.
   * 
   * @param conf
   * @param address
   * @return
   * @throws IOException
   */
  private Configuration getCrossSiteConf(Configuration conf, String clusterKey) throws IOException {
    // create the connection to the global zk
    Configuration crossSiteZKConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(crossSiteZKConf, clusterKey);
    return crossSiteZKConf;
  }

  private void finishSetup() throws IOException, KeeperException {
    this.zkw = new ZooKeeperWatcher(configuration, "connection to global zookeeper",
        new CrossSiteDummyAbortable(), false);
    this.znodes = new CrossSiteZNodes(zkw);
    this.tableNameAsString = Bytes.toString(tableName);
    // check the state, only ENABLED/DISABLED are allowed.
    TableState state = znodes.getTableState(tableNameAsString);
    if (!TableState.ENABLED.equals(state) && !TableState.DISABLED.equals(state)) {
      throw new TableAbnormalStateException(tableNameAsString + ":" + state);
    }
    this.autoFlush = true;
    this.maxKeyValueSize = this.configuration.getInt("hbase.client.keyvalue.maxsize", -1);
    this.scannerCaching = this.configuration.getInt("hbase.client.scanner.caching", 1);
    this.writeBufferSize = this.configuration.getLong("hbase.client.write.buffer", 2097152);

    this.tablesCache = new TreeMap<String, HTableInterface>();
    this.failover = this.configuration.getBoolean("hbase.crosssite.table.failover", false);

    loadZKInfo();
  }

  private void loadZKInfo() throws KeeperException, IOException {
    CachedZookeeperInfo cachedZKInfo = new CachedZookeeperInfo();
    cachedZKInfo.htd = znodes.getTableDesc(tableNameAsString);
    cachedZKInfo.clusterLocator = znodes.getClusterLocator(tableNameAsString);
    cachedZKInfo.clusterNames = getClusterNames();
    cachedZKInfo.clusterInfos = getClusterInfos(cachedZKInfo.clusterNames);
    cachedZKInfo.hierarchyMap = znodes.getHierarchyMap();
    this.cachedZKInfo = cachedZKInfo;
  }

  /**
   * For internal usage only. Refreshes the cached information of the CrossSiteHTable related with
   * the zookeeper, the cached tables won't be refreshed. This method could help to refresh the
   * cached zookeeper-related information without closing the underlying HTables. This will benefit
   * the usages of the internal components, for example the cross site thrift.
   * 
   * @throws IOException
   */
  public void refresh() throws IOException {
    try {
      loadZKInfo();
    } catch (KeeperException e) {
      LOG.error("Fail to load the zk info", e);
      throw new IOException(e);
    }
  }

  public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads = conf.getInt("hbase.crosssite.table.threads.max", Integer.MAX_VALUE);
    if (maxThreads <= 0) {
      maxThreads = Integer.MAX_VALUE;
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
    long keepAliveTime = conf.getLong("hbase.table.threads.keepalivetime", 60);
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
        blockingQueue, Threads.newDaemonThreadFactory("crosssite-hbase-table"), rejectHandler);
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getTableName() {
    return this.tableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return this.cachedZKInfo.htd;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Get get) throws IOException {
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    String clusterName = cachedZKInfo.clusterLocator.getClusterName(get.getRow());
    try {
      return getClusterHTable(clusterName).exists(get);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      LOG.warn("Fail to connect to the cluster " + clusterName, e);
      // Not do the failover for all IOException, only for the exceptions related with the connect
      // issue.
      if (failover && CrossSiteUtil.isFailoverException(e)) {
        LOG.warn("Failover: redirect the get request to the peers. Please notice, the data may be stale.");
        HTableInterface table = findAvailablePeer(cachedZKInfo.clusterInfos, clusterName);
        if (table == null) {
          LOG.error("Fail to find the peers", e);
          throw e;
        } else {
          try {
            return table.exists(get);
          } finally {
            try {
              table.close();
            } catch (IOException e1) {
              LOG.warn("Fail to close the peer HTable", e1);
            }
          }
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException,
      InterruptedException {
    if (results.length != actions.size()) {
      throw new IllegalArgumentException(
          "argument results must be the same size as argument actions");
    }
    if (actions.isEmpty()) {
      return;
    }
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    Map<String, Map<Integer, Row>> clusterMap = new TreeMap<String, Map<Integer,Row>>();
    Map<Integer, Object> rmap = new HashMap<Integer, Object>();
    int index = 0;
    for (Row action : actions) {
      String clusterName = clusterLocator.getClusterName(action.getRow());
      Map<Integer, Row> rows = clusterMap.get(clusterName);
      if (rows == null) {
        rows = new HashMap<Integer, Row>();
        clusterMap.put(clusterName, rows);
      }
      rows.put(Integer.valueOf(index++), action);
    }

    final AtomicBoolean hasError = new AtomicBoolean(false);
    Map<String, Future<Map<Integer, Object>>> futures = 
        new HashMap<String, Future<Map<Integer, Object>>>();
    for (final Entry<String, Map<Integer, Row>> entry : clusterMap.entrySet()) {
      futures.put(entry.getKey(), pool.submit(new Callable<Map<Integer, Object>>() {

        @Override
        public Map<Integer, Object> call() throws Exception {
          Map<Integer, Object> map = new HashMap<Integer, Object>();
          Map<Integer, Row> rowMap = entry.getValue();
          Object[] rs = new Object[rowMap.size()];
          List<Integer> indexes = new ArrayList<Integer>(rowMap.size());
          List<Row> rows = new ArrayList<Row>(rowMap.size());
          try {
            HTableInterface table = getClusterHTable(entry.getKey());
            for (Entry<Integer, Row> rowEntry : rowMap.entrySet()) {
              indexes.add(rowEntry.getKey());
              rows.add(rowEntry.getValue());
            }
            table.batch(rows, rs);
          } catch (IOException e) {
            // need clear the cached HTable if the connection is refused
            clearCachedTable(entry.getKey());
            hasError.set(true);
            LOG.error(e);
          } finally {
            int index = 0;
            for (Object r : rs) {
              map.put(indexes.get(index++), r);
            }
          }
          return map;
        }
      }));
    }

    try {
      for (Entry<String, Future<Map<Integer, Object>>> result : futures.entrySet()) {
        rmap.putAll(result.getValue().get());
      }
    } catch (Exception e) {
      // do nothing
    }

    for (int i = 0; i < actions.size(); i++) {
      results[i] = rmap.get(Integer.valueOf(i));
    }
    if (hasError.get()) {
      throw new IOException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    Object[] results = new Object[actions.size()];
    batch(actions, results);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(Get get) throws IOException {
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    String clusterName = cachedZKInfo.clusterLocator.getClusterName(get.getRow());
    try {
      return getClusterHTable(clusterName).get(get);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      LOG.warn("Fail to connect to the cluster " + clusterName, e);
      // Not do the failover for all IOException, only for the exceptions related with the connect
      // issue.
      if (failover && CrossSiteUtil.isFailoverException(e)) {
        LOG.warn("Failover: redirect the get request to the peers. Please notice, the data may be stale.");
        HTableInterface table = findAvailablePeer(cachedZKInfo.clusterInfos, clusterName);
        if (table == null) {
          LOG.error("Fail to find the peers", e);
          throw e;
        } else {
          try {
            return table.get(get);
          } finally {
            try {
              table.close();
            } catch (IOException e1) {
              LOG.warn("Fail to close the peer HTable", e1);
            }
          }
        }
      } else {
        throw e;
      }

    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    Map<String, Map<Integer, Get>> clusterMap = new TreeMap<String, Map<Integer, Get>>();
    Map<Integer, Result> results = new HashMap<Integer, Result>();
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    int index = 0;
    for (Get get : gets) {
      String clusterName = clusterLocator.getClusterName(get.getRow());
      Map<Integer, Get> getMap = clusterMap.get(clusterName);
      if(getMap == null) {
        getMap = new HashMap<Integer, Get>();
        clusterMap.put(clusterName, getMap);
      }
      getMap.put(Integer.valueOf(index++), get);
    }

    Map<String, Future<Map<Integer, Result>>> futures = 
        new HashMap<String, Future<Map<Integer, Result>>>();
    final Map<String, ClusterInfo> clusterInfos = cachedZKInfo.clusterInfos;
    for (final Entry<String, Map<Integer, Get>> entry : clusterMap.entrySet()) {
      futures.put(entry.getKey(), pool.submit(new Callable<Map<Integer, Result>>() {

        @Override
        public Map<Integer, Result> call() throws Exception {
          Map<Integer, Result> map = new TreeMap<Integer, Result>();
          try {
            HTableInterface table = getClusterHTable(entry.getKey());
            List<Get> gs = new ArrayList<Get>(entry.getValue().size());
            List<Integer> indexes = new ArrayList<Integer>(entry.getValue().size());
            for (Entry<Integer, Get> getEntry : entry.getValue().entrySet()) {
              indexes.add(getEntry.getKey());
              gs.add(getEntry.getValue());
            }
            Result[] rs = table.get(gs);
            int index = 0;
            for (Result r : rs) {
              map.put(indexes.get(index), r);
              index++;
            }
          } catch (IOException e) {
            // need clear the cached HTable if the connection is refused
            clearCachedTable(entry.getKey());
            LOG.warn("Fail to connect to the cluster " + entry.getKey(), e);
            // Not do the failover for all IOException, 
            // only for the exceptions related with the connect issue.
            if (failover && CrossSiteUtil.isFailoverException(e)) {
              LOG.warn("Failover: redirect the get request to the peers. Please notice, the data may be stale.");
              HTableInterface table = findAvailablePeer(clusterInfos, entry.getKey());
              if (table == null) {
                LOG.error("Fail to find any peers", e);
                throw e;
              } else {
                try {
                  List<Get> gs = new ArrayList<Get>(entry.getValue().size());
                  List<Integer> indexes = new ArrayList<Integer>(entry.getValue().size());
                  for (Entry<Integer, Get> getEntry : entry.getValue().entrySet()) {
                    indexes.add(getEntry.getKey());
                    gs.add(getEntry.getValue());
                  }
                  Result[] rs = table.get(gs);
                  int index = 0;
                  for (Result r : rs) {
                    map.put(indexes.get(index), r);
                    index++;
                  }
                } finally {
                  try {
                    table.close();
                  } catch (IOException e1) {
                    LOG.warn("Fail to close the peer HTable", e1);
                  }
                }
              }
            } else {
              throw e;
            }
          }
          return map;
        }

      }));
    }

    try {
      for (Entry<String, Future<Map<Integer, Result>>> result : futures.entrySet()) {
        results.putAll(result.getValue().get());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    Result[] rs = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      rs[i] = results.get(Integer.valueOf(i));
    }
    return rs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    String clusterName = cachedZKInfo.clusterLocator.getClusterName(row);
    try {
      return getClusterHTable(clusterName).getRowOrBefore(row, family);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      LOG.warn("Fail to connect to the cluster " + clusterName, e);
      // Not do the failover for all IOException, only for the exceptions related with the connect
      // issue.
      if (failover && CrossSiteUtil.isFailoverException(e)) {
        LOG.warn("Failover: redirect the get request to the peers. Please notice, the data may be stale.");
        HTableInterface table = findAvailablePeer(cachedZKInfo.clusterInfos, clusterName);
        if (table == null) {
          LOG.error("Fail to find the peers", e);
          throw e;
        } else {
          try {
            return table.getRowOrBefore(row, family);
          } finally {
            try {
              table.close();
            } catch (IOException e1) {
              LOG.warn("Fail to close the peer HTable", e1);
            }
          }
        }
      } else {
        throw e;
      }

    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(Scan scan, String[] clusterNames) throws IOException {
    if (scan.getCaching() <= 0) {
      scan.setCaching(getScannerCaching());
    }
    return new CrossSiteClientScanner(configuration, scan, tableName,
        getClusterInfoStartStopKeyPairs(scan.getStartRow(), scan.getStopRow(), clusterNames),
        failover, pool, znodes, hTableFactory);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return getScanner(scan, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(Put put) throws IOException {
    validatePut(put);
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(put.getRow());
    try {
      getClusterHTable(clusterName).put(put);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(List<Put> puts) throws IOException {
    Map<String, List<Put>> tableMap = new HashMap<String, List<Put>>();
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    for (Put put : puts) {
      validatePut(put);
      String clusterName = clusterLocator.getClusterName(put.getRow());
      List<Put> ps = tableMap.get(clusterName);
      if (ps == null) {
        ps = new ArrayList<Put>();
        tableMap.put(clusterName, ps);
      }
      ps.add(put);
    }
    Map<String, Future<Void>> futures = 
        new HashMap<String, Future<Void>>();
    for (final Entry<String, List<Put>> entry : tableMap.entrySet()) {
      futures.put(entry.getKey(), pool.submit(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          try {
            getClusterHTable(entry.getKey()).put(entry.getValue());
          } catch (IOException e) {
            // need clear the cached HTable if the connection is refused
            clearCachedTable(entry.getKey());
            throw e;
          }
          return null;
        }
      }));
    }
    boolean hasError = false;
    for (Entry<String, Future<Void>> result : futures.entrySet()) {
      try { 
        result.getValue().get();
      } catch (Exception e) {
        hasError = true;
        LOG.error(e);
      }
    }
    if (hasError) {
      throw new IOException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(row);
    try {
      return getClusterHTable(clusterName).checkAndPut(row, family, qualifier, value, put);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(Delete delete) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(delete.getRow());
    try {
      getClusterHTable(clusterName).delete(delete);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    Map<String, List<Delete>> tableMap = new HashMap<String, List<Delete>>();
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    for (Delete delete : deletes) {
      String clusterName = clusterLocator.getClusterName(delete.getRow());
      List<Delete> ds = tableMap.get(clusterName);
      if (ds == null) {
        ds = new ArrayList<Delete>();
        tableMap.put(clusterName, ds);
      }
      ds.add(delete);
    }
    Map<String, Future<Void>> futures = 
        new HashMap<String, Future<Void>>();
    for (final Entry<String, List<Delete>> entry : tableMap.entrySet()) {
      futures.put(entry.getKey(), pool.submit(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          try {
            getClusterHTable(entry.getKey()).delete(entry.getValue());
          } catch (IOException e) {
            // need clear the cached HTable if the connection is refused
            clearCachedTable(entry.getKey());
            throw e;
          }
          return null;
        }
      }));
    }
    boolean hasError = false;
    for (Entry<String, Future<Void>> result : futures.entrySet()) {
      try { 
        result.getValue().get();
      } catch (Exception e) {
        hasError = true;
        LOG.error(e);
      }
    }
    if (hasError) {
      throw new IOException();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(row);
    try {
      return getClusterHTable(clusterName).checkAndDelete(row, family, qualifier, value, delete);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(rm.getRow());
    try {
      getClusterHTable(clusterName).mutateRow(rm);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(Append append) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(append.getRow());
    try {
      return getClusterHTable(clusterName).append(append);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(Increment increment) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(increment.getRow());
    try {
      return getClusterHTable(clusterName).increment(increment);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      boolean writeToWAL) throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("column is null");
    }
    if (npe != null) {
      throw new IOException("Invalid arguments to incrementColumnValue", npe);
    }
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(row);
    try {
      return getClusterHTable(clusterName).incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    for (Entry<String, HTableInterface> entry : tablesCache.entrySet()) {
      if (entry.getValue() != null) {
        try {
          entry.getValue().flushCommits();
        } catch (IOException e) {
          clearCachedTable(entry.getKey());
          throw e;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    flushCommits();
    if (cleanupPoolOnClose) {
      this.pool.shutdown();
    }
    for (HTableInterface table : tablesCache.values()) {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          LOG.warn("Fail to close the HTable underneath the cross site table", e);
        }
      }
    }
    this.zkw.close();
    this.tablesCache.clear();
    this.cachedZKInfo.clear();
    this.closed = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(row);
    try {
      return getClusterHTable(clusterName).lockRow(row);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unlockRow(RowLock rl) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(rl.getRow());
    try {
      getClusterHTable(clusterName).unlockRow(rl);
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * Not supported.
   */
  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, Call<T, R> callable) throws IOException, Throwable {
    final Map<byte[], R> results = Collections.synchronizedMap(new TreeMap<byte[], R>(
        Bytes.BYTES_COMPARATOR));
    coprocessorExec(protocol, startKey, endKey, callable, new Batch.Callback<R>() {
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, String[] clusterNames, Call<T, R> callable)
      throws IOException, Throwable {
    final Map<byte[], R> results = Collections.synchronizedMap(new TreeMap<byte[], R>(
        Bytes.BYTES_COMPARATOR));
    coprocessorExec(protocol, startKey, endKey, clusterNames, callable, new Batch.Callback<R>() {
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(final Class<T> protocol,
      final byte[] startKey, final byte[] endKey, final Call<T, R> callable,
      final Callback<R> callback) throws IOException, Throwable {
    coprocessorExec(protocol, startKey, endKey, null, callable, callback);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(final Class<T> protocol,
      final byte[] startKey, final byte[] endKey, final String[] clusterNames,
      final Call<T, R> callable, final Callback<R> callback) throws IOException, Throwable {
    List<Pair<ClusterInfo, Pair<byte[], byte[]>>> cis = getClusterInfoStartStopKeyPairs(startKey,
        endKey, clusterNames);
    Map<String, Future<Void>> futures = new HashMap<String, Future<Void>>();
    for (final Pair<ClusterInfo, Pair<byte[], byte[]>> pair : cis) {
      futures.put(pair.getFirst().getName(), pool.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            HTableInterface table = getClusterHTable(pair.getFirst().getName(), pair.getFirst()
                .getAddress());
            table.coprocessorExec(protocol, pair.getSecond().getFirst(), pair.getSecond()
                .getSecond(), callable, callback);
          } catch (Throwable e) {
            throw new Exception(e);
          }
          return null;
        }
      }));
    }
    try {
      for (Entry<String, Future<Void>> result : futures.entrySet()) {
        result.getValue().get();
      }
    } catch (Exception e) {
      // do nothing. Even the exception occurs, we regard the cross site
      // HTable has been deleted
      LOG.error("Fail to execute the coprocessor in the cross site table " + tableName, e);
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    this.autoFlush = autoFlush;
    this.clearBufferOnFail = autoFlush || clearBufferOnFail;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    for (HTableInterface table : tablesCache.values()) {
      try {
        table.setWriteBufferSize(this.writeBufferSize);
      } catch (IOException e) {
        LOG.warn("Fail to set write buffer size to the table", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public void prewarmRegionCache(Map<HRegionInfo, HServerAddress> regionMap) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout = operationTimeout;
    for (HTableInterface table : tablesCache.values()) {
      if (table instanceof HTable) {
        ((HTable) table).setOperationTimeout(this.operationTimeout);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getOperationTimeout() {
    return this.operationTimeout;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HRegionLocation getRegionLocation(String row) throws IOException {
    return this.getRegionLocation(Bytes.toBytes(row));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HRegionLocation getRegionLocation(byte[] row) throws IOException {
    return this.getRegionLocation(row, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HRegionLocation getRegionLocation(byte[] row, boolean reload) throws IOException {
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String clusterName = clusterLocator.getClusterName(row);
    try {
      HTableInterface table = getClusterHTable(clusterName);
      if (table instanceof HTable) {
        return ((HTable) table).getRegionLocation(row, reload);
      }
      throw new UnsupportedOperationException();
    } catch (IOException e) {
      // need clear the cached HTable if the connection is refused
      clearCachedTable(clusterName);
      throw e;
    }
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public HConnection getConnection() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public byte[][] getStartKeys() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public byte[][] getEndKeys() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    // @deprecated Use {@link #getRegionLocations()} or {@link #getStartEndKeys()}
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    List<ClusterInfo> cis = getClusterInfos(null, null);
    List<Future<NavigableMap<HRegionInfo, ServerName>>> futures = 
        new ArrayList<Future<NavigableMap<HRegionInfo, ServerName>>>();
    for (final ClusterInfo ci : cis) {
      futures.add(pool.submit(new Callable<NavigableMap<HRegionInfo, ServerName>>() {
        @Override
        public NavigableMap<HRegionInfo, ServerName> call() throws Exception {
          boolean closeTable = false;
          HTableInterface table = tablesCache.get(ci.getName());
          if (table == null) {
            // Not cached.Let us create one. Do not cache this created one and make sure to close
            // this when the operation is done
            table = createHTable(ci.getName(), ci.getAddress());
            closeTable = true;
          }
          try {
            if (table instanceof HTable) {
              return ((HTable) table).getRegionLocations();
            }
            throw new UnsupportedOperationException();
          } finally {
            if (closeTable) {
              try {
                table.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin", e);
              }
            }
          }
        }
      }));
    }
    try {
      NavigableMap<HRegionInfo, ServerName> regionLocations = new TreeMap<HRegionInfo, ServerName>();
      for (Future<NavigableMap<HRegionInfo, ServerName>> result : futures) {
        if (result != null) {
          NavigableMap<HRegionInfo, ServerName> regions = result.get();
          if (regions != null) {
            regionLocations.putAll(regions);
          }
        }
      }
      return regionLocations;
    } catch (Exception e) {
      // do nothing. Even the exception occurs, we regard the cross site
      // HTable has been deleted
      LOG.error("Fail to get region locations of the cross site table " + tableName, e);
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<HRegionLocation> getRegionsInRange(byte[] startKey, byte[] endKey) throws IOException {
    return this.getRegionsInRange(startKey, endKey, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<HRegionLocation> getRegionsInRange(final byte[] startKey, final byte[] endKey,
      final boolean reload) throws IOException {
    List<ClusterInfo> cis = getClusterInfos(startKey, endKey);
    List<Future<List<HRegionLocation>>> futures = new ArrayList<Future<List<HRegionLocation>>>();
    for (final ClusterInfo ci : cis) {
      futures.add(pool.submit(new Callable<List<HRegionLocation>>() {
        @Override
        public List<HRegionLocation> call() throws Exception {
          boolean closeTable = false;
          HTableInterface table = tablesCache.get(ci.getName());
          if (table == null) {
            // Not cached.Let us create one. Do not cache this created one and make sure to close
            // this when the operation is done
            table = createHTable(ci.getName(), ci.getAddress());
            closeTable = true;
          }
          try {
            if (table instanceof HTable) {
              return ((HTable) table).getRegionsInRange(startKey != null ? startKey
                  : HConstants.EMPTY_START_ROW, endKey != null ? endKey : HConstants.EMPTY_END_ROW,
                  reload);
            }
            throw new UnsupportedOperationException();
          } finally {
            if (closeTable) {
              try {
                table.close();
              } catch (IOException e) {
                LOG.warn("Fail to close the HBaseAdmin", e);
              }
            }
          }
        }
      }));
    }
    try {
      List<HRegionLocation> regionLocations = new ArrayList<HRegionLocation>();
      for (Future<List<HRegionLocation>> result : futures) {
        if (result != null) {
          List<HRegionLocation> regions = result.get();
          if (regions != null) {
            regionLocations.addAll(regions);
          }
        }
      }
      return regionLocations;
    } catch (Exception e) {
      // do nothing. Even the exception occurs, we regard the cross site
      // HTable has been deleted
      LOG.error("Fail to get region locations in a range of the cross site table " + tableName, e);
      throw new IOException(e);
    }
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public ArrayList<Put> getWriteBuffer() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc} Not supported.
   */
  @Override
  public void clearRegionCache() {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets all the cluster names.
   * 
   * @return
   * @throws KeeperException
   */
  private Set<String> getClusterNames() throws KeeperException {
    List<String> clusterNames = znodes.listClusters();
    TreeSet<String> cns = new TreeSet<String>();
    if (clusterNames != null) {
      cns.addAll(clusterNames);
    }
    return cns;
  }

  /**
   * Gets the HTable for the cluster. The HTable is cached in the cross site table until it's
   * closed, or the HTable is not reachable.
   * 
   * @param clusterName
   * @return
   * @throws IOException
   */
  public HTableInterface getClusterHTable(String clusterName) throws IOException {
    // find the cluster address
    HTableInterface table = tablesCache.get(clusterName);
    if (table == null) {
      String clusterAddress = null;
      try {
        clusterAddress = znodes.getClusterAddress(clusterName);
      } catch (KeeperException e) {
        throw new IOException(e);
      }
      table = createHTable(clusterName, clusterAddress);
      tablesCache.put(clusterName, table);
    }
    return table;
  }

  private HTableInterface createHTable(String clusterName, String clusterAddress)
      throws IOException {
    Configuration clusterConf = getCrossSiteConf(this.configuration, clusterAddress);
    HTableInterface table = null;
    try {
      table = this.hTableFactory.createHTableInterface(clusterConf,
          Bytes.toBytes(CrossSiteUtil.getClusterTableName(tableNameAsString, clusterName)),
          this.pool);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e);
      }
    }
    table.setWriteBufferSize(this.writeBufferSize);
    table.setAutoFlush(autoFlush, clearBufferOnFail);
    if (table instanceof HTable && operationTimeout != 1) {
      ((HTable) table).setOperationTimeout(operationTimeout);
    }
    return table;
  }

  private HTableInterface getClusterHTable(String clusterName, String clusterAddress)
      throws IOException {
    HTableInterface table = tablesCache.get(clusterName);
    if (table == null) {
      table = createHTable(clusterName, clusterAddress);
      tablesCache.put(clusterName, table);
    }
    return table;
  }

  private void validatePut(final Put put) throws IllegalArgumentException {
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * Finds the available peer. This peer is not cached.
   * 
   * @param clusterInfos
   * @param clusterName
   * @return the available peer, returns null if no available ones.
   */
  private HTableInterface findAvailablePeer(Map<String, ClusterInfo> clusterInfos,
      String clusterName) {
    ClusterInfo ci = clusterInfos.get(clusterName);
    if (ci != null) {
      for (ClusterInfo peer : ci.getPeers()) {
        try {
          Configuration clusterConf = getCrossSiteConf(this.configuration, peer.getAddress());
          HTableInterface table = null;
          try {
            table = this.hTableFactory.createHTableInterface(clusterConf, Bytes
                .toBytes(CrossSiteUtil.getPeerClusterTableName(tableNameAsString, clusterName,
                    peer.getName())));
          } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            } else {
              throw new IOException(e);
            }
          }
          if (operationTimeout != 1) {
            if (table instanceof HTable) {
              ((HTable) table).setOperationTimeout(operationTimeout);
            }
          }
          LOG.info("Found a peer " + peer + " for " + clusterName);
          return table;
        } catch (Exception e) {
          LOG.info("Fail to contact the peer " + peer, e);
        }
      }
    }
    return null;
  }

  /**
   * Gets the map of the <code>ClusterInfo</code>. The key is the cluster name.
   * 
   * @param clusterNames
   * @return
   * @throws KeeperException
   */
  private Map<String, ClusterInfo> getClusterInfos(Set<String> clusterNames) throws KeeperException {
    Map<String, ClusterInfo> clusterInfos = new TreeMap<String, ClusterInfo>();
    for (String clusterName : clusterNames) {
      String address = znodes.getClusterAddress(clusterName);
      List<ClusterInfo> peerList = znodes.getPeerClusters(clusterName);
      Set<ClusterInfo> peers = null;
      if (peerList != null) {
        peers = new TreeSet<ClusterInfo>();
        peers.addAll(peerList);
      }
      ClusterInfo ci = new ClusterInfo(clusterName, address, peers);
      clusterInfos.put(clusterName, ci);
    }
    return clusterInfos;
  }

  /**
   * Gets all the physical clusters by the start/stop rows.
   * 
   * @param start
   * @param stop
   * @return the list of the <code>ClusterInfo</code>, returns an empty one if there're not 
   *         clusters between the start/stop rows.
   * @throws IOException
   */
  private List<ClusterInfo> getClusterInfos(byte[] start, byte[] stop)
      throws IOException {
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    // find the clusters.
    Set<String> clusterNames = cachedZKInfo.clusterNames;
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    // TODO when the Locator is CompositeSubstringClusterLocator or SubstringClusterLocator and
    // index is 0, we can extract the cluster names?
    if (clusterLocator instanceof PrefixClusterLocator) {
      // if the locator is a prefix locator, do the optimization.
      // find the start row and stop row in the scan, and find the cluster
      // names.
      clusterNames = filterClustersForPrefixClusterLocator((PrefixClusterLocator) clusterLocator,
          clusterNames, start, stop, false);
    }

    // clusters indicated in the parameter.
    List<ClusterInfo> results = new ArrayList<ClusterInfo>();
    for (String clusterName : clusterNames) {
      ClusterInfo ci = cachedZKInfo.clusterInfos.get(clusterName);
      if (ci != null) {
        results.add(ci);
      }
    }
    return results;
  }

  /**
   * Gets the list of the pair of the <code>ClusterInfo</code> and start/stop keys.
   * <ul>
   * <li>If the clusters are specified, the start/stop key for the clusterA would be changed to
   * ClusterA + delimiter + start/stop key.</li>
   * <li>If the clusters are not specified, the start/stop key won't be changed.</li>
   * </ul>
   * 
   * @param start
   * @param stop
   * @param clusters
   * @return
   * @throws IOException
   */
  private List<Pair<ClusterInfo, Pair<byte[], byte[]>>> getClusterInfoStartStopKeyPairs(
      byte[] start, byte[] stop, String[] clusters) throws IOException {
    CachedZookeeperInfo cachedZKInfo = this.cachedZKInfo;
    // find the clusters.
    Set<String> clusterNames = null;
    boolean shouldAppendClusterName = false;
    if (clusters == null || clusters.length == 0) {
      clusterNames = cachedZKInfo.clusterNames;
    } else {
      // Hierarchy should be got in any case
      clusterNames = getPhysicalClusters(cachedZKInfo, clusters);
      shouldAppendClusterName = true;
    }
    ClusterLocator clusterLocator = cachedZKInfo.clusterLocator;
    String delimiter = null;
    // TODO when the Locator is CompositeSubstringClusterLocator or SubstringClusterLocator and
    // index is 0, we can extract the cluster names?
    if (clusterLocator instanceof PrefixClusterLocator) {
      // if the locator is a prefix locator, do the optimization.
      // find the start row and stop row in the scan, and find the cluster
      // names.
      PrefixClusterLocator prefixClusterLocator = (PrefixClusterLocator) clusterLocator;
      delimiter = prefixClusterLocator.getDelimiter();
      if (shouldAppendClusterName) {
        clusterNames = filterClustersForPrefixClusterLocator(prefixClusterLocator, clusterNames,
            start, stop, true);
      } else {
        clusterNames = filterClustersForPrefixClusterLocator(prefixClusterLocator, clusterNames,
            start, stop, false);
      }
    } else {
      shouldAppendClusterName = false;
    }

    // clusters indicated in the parameter.
    List<Pair<ClusterInfo, Pair<byte[], byte[]>>> results = 
        new ArrayList<Pair<ClusterInfo, Pair<byte[], byte[]>>>();
    for (String clusterName : clusterNames) {
      ClusterInfo ci = cachedZKInfo.clusterInfos.get(clusterName);
      if (ci != null) {
        Pair<byte[], byte[]> keyPair = new Pair<byte[], byte[]>(getStartKey(
            shouldAppendClusterName, clusterName, delimiter, start), getStopKey(
            shouldAppendClusterName, clusterName, delimiter, stop));
        results.add(new Pair<ClusterInfo, Pair<byte[], byte[]>>(ci, keyPair));
      }
    }
    return results;
  }

  private byte[] getStartKey(boolean shouldAppendClusterName, String clusterName, String delimiter,
      byte[] startKey) {
    if (shouldAppendClusterName && startKey != null
        && !Bytes.equals(startKey, HConstants.EMPTY_START_ROW)) {
      String sk = Bytes.toString(startKey);
      return Bytes.toBytes(clusterName + delimiter + sk);
    } else {
      return startKey;
    }
  }

  private byte[] getStopKey(boolean shouldAppendClusterName, String clusterName, String delimiter,
      byte[] stopKey) {
    if (shouldAppendClusterName && stopKey != null
        && !Bytes.equals(stopKey, HConstants.EMPTY_END_ROW)) {
      String sk = Bytes.toString(stopKey);
      return Bytes.toBytes(clusterName + delimiter + sk);
    } else {
      return stopKey;
    }
  }

  /**
   * Gets all the physical clusters.
   * 
   * @param cachedZKInfo
   * @param clusters
   * @return
   */
  private Set<String> getPhysicalClusters(CachedZookeeperInfo cachedZKInfo, String[] clusters) {
    Set<String> results = new TreeSet<String>();
    if (clusters != null) {
      for (String cluster : clusters) {
        results.add(cluster);
        results.addAll(CrossSiteUtil.getDescendantClusters(cachedZKInfo.hierarchyMap, cluster));
      }
    }
    for (Iterator<String> ir = results.iterator(); ir.hasNext();) {
      if (!cachedZKInfo.clusterNames.contains(ir.next())) {
        ir.remove();
      }
    }
    return results;
  }

  /**
   * Clears the cached table.
   * 
   * @param clusterName
   */
  private void clearCachedTable(String clusterName) {
    try {
      HTableInterface t = this.tablesCache.get(clusterName);
      if (t != null) {
        t.close();
      }
    } catch (IOException e1) {
      LOG.warn("Fail to close the table of " + clusterName, e1);
    }
    tablesCache.remove(clusterName);
  }

  /**
   * Gets all the clusters by the start/stop rows when the cluster locator is PrefixClusterLocator.
   * 
   * @param prefixClusterLocator
   * @param clusters
   * @param start
   * @param stop
   * @param skipFilter
   * @return
   */
  private Set<String> filterClustersForPrefixClusterLocator(
      PrefixClusterLocator prefixClusterLocator, Set<String> clusters, byte[] start, byte[] stop,
      boolean skipFilter) {
    if (skipFilter) {
      return clusters;
    }
    String startCluster = null;
    String stopCluster = null;
    try {
      if (start != null && !Bytes.equals(start, HConstants.EMPTY_START_ROW)) {
        startCluster = Bytes.toString(start);
        if (startCluster.indexOf(prefixClusterLocator.getDelimiter()) >= 0) {
          startCluster = prefixClusterLocator.getClusterName(start);
        }
      }
    } catch (RowNotLocatableException e) {
      // do nothing
    }
    try {
      if (stop != null && !Bytes.equals(stop, HConstants.EMPTY_END_ROW)) {
        stopCluster = Bytes.toString(stop);
        if (stopCluster.indexOf(prefixClusterLocator.getDelimiter()) >= 0) {
          stopCluster = prefixClusterLocator.getClusterName(stop);
        }
      }
    } catch (RowNotLocatableException e) {
      // do nothing
    }
    // find the start row and stop row in the scan, and find the cluster name
    if (startCluster == null && stopCluster == null) {
      return clusters;
    } else {
      Set<String> results = new TreeSet<String>();
      if (startCluster == null) {
        for (String cluster : clusters) {
          if (cluster.compareTo(stopCluster) <= 0) {
            results.add(cluster);
          } else {
            break;
          }
        }
      } else if (stopCluster == null) {
        boolean skip = true;
        for (String cluster : clusters) {
          if (!skip) {
            results.add(cluster);
          } else {
            if (cluster.compareTo(startCluster) >= 0) {
              results.add(cluster);
              skip = false;
            }
          }
        }
      } else {
        boolean more = false;
        for (String cluster : clusters) {
          if (!more) {
            if (cluster.compareTo(startCluster) >= 0) {
              more = true;
              if (cluster.compareTo(stopCluster) <= 0) {
                results.add(cluster);
              } else {
                break;
              }
            }
          } else {
            if (cluster.compareTo(stopCluster) <= 0) {
              results.add(cluster);
            } else {
              break;
            }
          }
        }
      }
      return results;
    }
  }

  /**
   * A cached zookeeper information.
   */
  private class CachedZookeeperInfo {
    public HTableDescriptor htd;
    public ClusterLocator clusterLocator;
    public Set<String> clusterNames;
    public Map<String, ClusterInfo> clusterInfos;
    public Map<String, Set<String>> hierarchyMap;

    public void clear() {
      if (clusterNames != null) {
        clusterNames.clear();
      }
      if (clusterInfos != null) {
        clusterInfos.clear();
      }
      if (hierarchyMap != null) {
        hierarchyMap.clear();
      }
    }
  }
}

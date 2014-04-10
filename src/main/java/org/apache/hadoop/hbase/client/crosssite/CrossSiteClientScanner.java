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
package org.apache.hadoop.hbase.client.crosssite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MergeSortIterator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * Implements the scanner interface for partition table. This scanner will iterate all partition
 * segment tables and return the results in universal order.
 */
class CrossSiteClientScanner implements ResultScanner {
  private static final Log LOG = LogFactory.getLog(CrossSiteClientScanner.class);

  private final Configuration configuration;
  private final Scan scan;
  private final String tableName;
  private final ExecutorService pool;
  private boolean ignoreUnavailableClusters;
  private List<ClusterInfo> clusters;
  private List<ScannerIterator> clusterScannerIterators;
  private Iterator<Result> resultIterator;
  private boolean closed = false;
  private boolean failover;
  private CrossSiteZNodes znodes;
  private final HTableInterfaceFactory hTableFactory;

  protected CrossSiteClientScanner(final Configuration conf, final Scan scan,
      final byte[] tableName, List<ClusterInfo> clusters, boolean failover, ExecutorService pool,
      CrossSiteZNodes znodes, HTableInterfaceFactory hTableFactory) throws IOException {
    this.configuration = conf;
    this.scan = scan;
    this.tableName = Bytes.toString(tableName);
    this.pool = pool;
    this.ignoreUnavailableClusters = configuration.getBoolean(
        CrossSiteConstants.CROSS_SITE_TABLE_SCAN_IGNORE_UNAVAILABLE_CLUSTERS, false);
    this.failover = failover;
    this.clusters = clusters;
    this.znodes = znodes;
    clusterScannerIterators = new ArrayList<ScannerIterator>();
    this.hTableFactory = hTableFactory;
    initialize();
  }

  private void initialize() throws IOException {
    int count = this.clusters.size();
    List<Future<ScannerIterator>> futures = new ArrayList<Future<ScannerIterator>>();
    for (int i = count - 1; i >= 0; i--) {
      Callable<ScannerIterator> callable = createCallable(clusters.get(i), this.tableName,
          ignoreUnavailableClusters);
      if (callable != null) {
        futures.add(pool.submit(callable));
      }
    }

    IOException exception = null;
    for (Future<ScannerIterator> future : futures) {
      try {
        ScannerIterator iter = future.get();
        if (iter != null) {
          clusterScannerIterators.add(iter);
        }
      } catch (InterruptedException e) {
        exception = new IOException("Interrupted", e);
      } catch (ExecutionException e) {
        exception = new IOException(e.getCause());
      }
    }
    if (exception != null) {
      close();
      // just throw the last exception
      throw exception;
    }
    if (clusterScannerIterators.size() == 0) {
      // add an empty scanner iterator
      LOG.debug("The ScannerIterator is empty, the EmptyScannerIterator is used instead");
      clusterScannerIterators.add(new EmptyScannerIterator());
    }
    this.resultIterator = new MergeSortIterator<Result>(clusterScannerIterators,
        new ResultComparator());
  }

  private Callable<ScannerIterator> createCallable(final ClusterInfo cluster,
      final String tableName, final boolean ignore) {
    return new Callable<ScannerIterator>() {
      @Override
      public ScannerIterator call() throws Exception {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Start initialization of scanner" + " for the cluster " + cluster);
        }
        String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, cluster.getName());
        HTableInterface table = null;
        try {
          try {
            table = hTableFactory.createHTableInterface(
                getClusterConf(configuration, cluster.getAddress()), Bytes.toBytes(clusterTableName)); 
          } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            } else {
              throw new IOException(e);
            }
          }
        } catch (IOException e) {
          LOG.info("Fail to connect to the CSBTable " + tableName + " in cluster " + cluster, e);
          if (failover && CrossSiteUtil.isFailoverException(e)) {
            LOG.warn("Start to failover to the peers for cluster " + cluster
                + ". Please notice, the data may be stale.");
            // Get the peers once from the zk.. this would be helpful when a new peer is added after
            // a CrossSiteHTable instance has been created
            List<ClusterInfo> peerClusters = znodes.getPeerClusters(cluster.getName());
            for (ClusterInfo peerCluster : peerClusters) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Start failover to the peer cluster" + peerCluster + " for the cluster "
                    + cluster);
              }
              Configuration conf = getClusterConf(configuration, peerCluster.getAddress());
              try {
                String peerClusterTableName = CrossSiteUtil.getPeerClusterTableName(tableName,
                    cluster.getName(), peerCluster.getName());
                try {
                  table = hTableFactory.createHTableInterface(conf,
                      Bytes.toBytes(peerClusterTableName));
                } catch (RuntimeException re) {
                  if (re.getCause() instanceof IOException) {
                    throw (IOException) re.getCause();
                  } else {
                    throw new IOException(re);
                  }
                }

                LOG.info("Failover to the cluster " + peerCluster
                    + ". Please notice, the data may be stale.");
                break;
              } catch (IOException ioe) {
                LOG.warn("Failed to connect to peer cluster '" + peerCluster
                    + "'. Will try other peers", ioe);
              }
            }
          } else {
            if (ignore) {
              LOG.warn("The scanner for the cluster " + cluster + " will be ignored");
              return null;
            } else {
              throw new IOException("Failed to initialize CSBTable '" + tableName + "' in cluster "
                  + cluster);
            }
          }
        }

        if (table == null) {
          if (!ignore) {
            throw new IOException("Failed to initialize CSBTable '" + tableName
                + "' in main and peer clusters");
          } else {
            LOG.warn("The scanner for the cluster " + cluster.getName() + " will be ignored");
            return null;
          }
        }
        return new ScannerIterator(table, new Scan(scan));
      }
    };
  }

  @Override
  public void close() {
    if (closed)
      return;
    for (ScannerIterator it : clusterScannerIterators) {
      it.close();
    }
    this.closed = true;
  }

  @Override
  public Result next() throws IOException {
    if (this.closed)
      return null;

    try {
      return resultIterator.next();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public Iterator<Result> iterator() {
    return resultIterator;
  }

  private static class ResultComparator implements Comparator<Result> {
    @Override
    public int compare(Result r1, Result r2) {
      if (r1 == null && r2 != null) {
        return 1;
      } else if (r1 == null && r2 == null) {
        return 0;
      } else if (r1 != null && r2 == null) {
        return -1;
      }

      return Bytes.compareTo(r1.getRow(), r2.getRow());
    }
  }

  private class ScannerIterator implements Iterator<Result> {
    private HTableInterface table;
    private ResultScanner scanner;
    private Result next = null;
    boolean closed = false;
    private Future<Result> future;

    protected ScannerIterator() {
    }

    protected ScannerIterator(HTableInterface table, Scan s) throws IOException {
      this.table = table;
      this.scanner = table.getScanner(s);
      nextInternal();
    }

    public void close() {
      if (closed)
        return;
      this.scanner.close();
      try {
        this.table.close();
      } catch (IOException e) {
        LOG.error("Exception while closing table '" + Bytes.toString(table.getTableName()), e);
      }
      this.closed = true;
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        try {
          next = future == null ? null : future.get();
          nextInternal();
          return next != null;
        } catch (Throwable t) {
          CrossSiteClientScanner.this.close();
          throw new RuntimeException(t instanceof ExecutionException ? t.getCause() : t);
        }
      }
      return true;
    }

    private void nextInternal() {
      if (closed) {
        future = null;
      } else {
        future = pool.submit(new Callable<Result>() {
          @Override
          public Result call() throws Exception {
            Result ret = scanner.next();
            if (ret == null) {
              close();
            }
            return ret;
          }
        });
      }
    }

    @Override
    public Result next() {
      // since hasNext() does the real advancing, we call this to determine
      // if there is a next before proceeding.
      if (!hasNext()) {
        return null;
      }

      // if we get to here, then hasNext() has given us an item to return.
      // we want to return the item and then null out the next pointer, so
      // we use a temporary variable.
      Result temp = next;
      next = null;
      return temp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * An empty implementation of the ScannerIterator
   */
  private class EmptyScannerIterator extends ScannerIterator {

    public EmptyScannerIterator() throws IOException {
      super();
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Result next() {
      return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Configuration getClusterConf(Configuration conf, String address) throws IOException {
    Configuration otherConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(otherConf, address);
    return otherConf;
  }
}
// TODO to handle the scan metric which are passed via Scan op attrs. Follow the work happening
// around this in HBASE-9272
// Why we need a clinet side parallel scanner with merge sort always? Atleast in Prefix locator?
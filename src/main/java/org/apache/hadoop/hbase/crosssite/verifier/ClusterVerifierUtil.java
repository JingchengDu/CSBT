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
package org.apache.hadoop.hbase.crosssite.verifier;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHBaseAdmin;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * Util class that is used by the CSBTClusterVerifier
 * 
 */
public class ClusterVerifierUtil {
  private static final Log LOG = LogFactory.getLog(ClusterVerifierUtil.class);

  /**
   * Helps to create the missing tables in the individual cluster along with
   * creaeting them in the peers. This is similar to the createTableInternal in
   * CrossSiteHBaseAdmin except that it does not create the table znodes and
   * only creates the tables using the individual cluster admin.
   * 
   * @param clusterLocator
   * @param tableSplitKeys
   * @param tableDesc
   * @param clusterName
   * @param ci
   * @throws IOException
   */
  public static void createTableInCluster(HTableDescriptor desc, byte[][] tableSplitKeys,
      ClusterLocator clusterLocator, String clusterName, ClusterInfo ci, CrossSiteHBaseAdmin crossSiteHBaseAdmin) throws IOException {
    String tableName = desc.getNameAsString();
    boolean createTableInPeers = isReplicatedTable(desc);
    HBaseAdmin admin = createHBaseAmin(crossSiteHBaseAdmin.getConfiguration(), ci.getAddress());
    String clusterTableName = CrossSiteUtil.getClusterTableName(tableName, clusterName);
    HTableDescriptor htd = new HTableDescriptor(desc);
    htd.setName(Bytes.toBytes(clusterTableName));
    byte[][] newSplitKeys = clusterLocator.getSplitKeys(clusterName, tableSplitKeys);
    createTable(clusterName, admin, tableName, htd, newSplitKeys);
    if (createTableInPeers) {
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
          HBaseAdmin peerAdmin = createHBaseAmin(crossSiteHBaseAdmin.getConfiguration(),
              peer.getAddress());
          String peerTableName = CrossSiteUtil.getPeerClusterTableName(tableName, clusterName,
              peer.getName());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Creating table " + peerTableName + " at peer " + peer);
          }
          try {
            boolean peerTableExists = peerAdmin.tableExists(peerTableName);
            if (!peerTableExists) {
              LOG.debug("The table " + peerTableName + " does not exist.  Hence creating");
              peerHtd.setName(Bytes.toBytes(peerTableName));
              createTable(peer.getName(), peerAdmin, peerTableName, peerHtd, newSplitKeys);
            } else {
              LOG.debug("The table " + peerTableName + " available already.");
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created table " + clusterTableName + " in the cluster " + clusterName);
    }
    LOG.info("The cross site table " + desc.getNameAsString() + " is created");
    // add the znodes to the {tableName}.
    return;
    
  }
  private static void createTable(final String clusterName, HBaseAdmin admin,
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
  /**
   * Creates HBaseAdmin
   * @param baseConf
   * @param clusterAddress
   * @return
   * @throws IOException
   */
  public static HBaseAdmin createHBaseAmin(Configuration baseConf, String clusterAddress)
      throws IOException {
    Configuration clusterConf = new Configuration(baseConf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    return new HBaseAdmin(clusterConf);
  }
  
  /**
   * Replicated table
   * @param htd
   * @return
   */
  public static boolean isReplicatedTable(HTableDescriptor htd) {
    boolean replicationEnabled = false;
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      replicationEnabled = (hcd.getScope() > 0);
      if (replicationEnabled) {
        break;
      }
    }
    return replicationEnabled;
  }
  
  public static void enableTable(HBaseAdmin admin, String tableName) throws IOException {
    try {
      admin.enableTable(tableName);
    } catch (TableNotDisabledException e) {
      // Supress the TableNotDisabledException.
    }
  }
  
  public static void disableTable(HBaseAdmin admin, String tableName) throws IOException {
    try {
      admin.disableTable(tableName);
    } catch (TableNotEnabledException e) {
      // Supress the TableNotEnabledException.
    }
  }
}

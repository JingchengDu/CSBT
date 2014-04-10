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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocatorRPCObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * The operations in the cross site table are included.
 */
public class CrossSiteZNodes {

  private static final String EPHEMERAL = "-ephemeral";
  private static final String CLUSTER_EPHEMERAL = "-cluster-ephemeral";
  private static final Log LOG = LogFactory.getLog(CrossSiteZNodes.class);
  public final static String CROSS_SITE_ZNODE_NAME = "CrossSite";
  public final static String CLUSTERS_ZNODE_NAME = "clusters";
  public final static String TABLES_ZNODE_NAME = "tables";
  public final static String CLUSTER_ADDRESS_ZNODE_NAME = "address";
  public final static String CLUSTER_PEERS_ZNODE_NAME = "peers";
  public final static String TABLE_STATE_ZNODE_NAME = "state";
  public final static String TABLE_DESC_ZNODE_NAME = "desc";
  public final static String TABLE_PROPOSED_DESC_ZNODE_NAME = "proposed_desc";
  public final static String TABLE_SPLITKEYS_ZNODE_NAME = "splitkeys";
  public final static String CLUSTER_LOCATOR_ZNODE_NAME = "locator";
  public final static String HIERARCHY_ZNODE_NAME = "hierarchy";

  private final ZooKeeperWatcher zkw;
  private String crossSiteZNode;
  private String clustersZNode;
  private String tablesZNode;
  private String hierarchyZNode;

  /** The state of the Cross Site Table **/
  // When adding new states later, make sure to add towards the end without disturbing the ordinal.
  public static enum TableState {
    ENABLED, 
    DISABLED, 
    ENABLING, 
    DISABLING, 
    DELETING, 
    MODIFYING, 
    ADDINGCOLUMN, 
    DELETINGCOLUMN, 
    MODIFYINGCOLUMN;
  };
  
  /**
   * 
   * @param zkw
   * @param createBaseZNode
   *          If true, the base znodes(CrossSite, clusters, tables and hierarchy) will be created if
   *          they're not present.
   * @throws KeeperException
   */
  public CrossSiteZNodes(ZooKeeperWatcher zkw, boolean createBaseZNode) throws KeeperException {
    this.zkw = zkw;
    this.crossSiteZNode = ZKUtil.joinZNode(zkw.baseZNode, CROSS_SITE_ZNODE_NAME);
    this.clustersZNode = ZKUtil.joinZNode(crossSiteZNode, CLUSTERS_ZNODE_NAME);
    this.tablesZNode = ZKUtil.joinZNode(crossSiteZNode, TABLES_ZNODE_NAME);
    this.hierarchyZNode = ZKUtil.joinZNode(crossSiteZNode, HIERARCHY_ZNODE_NAME);
    if (createBaseZNode) {
      if (ZKUtil.checkExists(zkw, crossSiteZNode) == -1) {
        ZKUtil.createWithParents(zkw, crossSiteZNode);
      }
      if (ZKUtil.checkExists(zkw, clustersZNode) == -1) {
        ZKUtil.createWithParents(zkw, clustersZNode);
      }
      if (ZKUtil.checkExists(zkw, tablesZNode) == -1) {
        ZKUtil.createWithParents(zkw, tablesZNode);
      }
      if (ZKUtil.checkExists(zkw, hierarchyZNode) == -1) {
        ZKUtil.createWithParents(zkw, hierarchyZNode);
      }
    }
  }

  /**
   * The constructor. The base znodes(CrossSite, clusters, tables and hierarchy) will be created if
   * they're not present.
   * 
   * @param zkw
   * @throws KeeperException
   */
  public CrossSiteZNodes(ZooKeeperWatcher zkw) throws KeeperException {
    this(zkw, true);
  }

  /**
   * @return the root znode for the cross site tables.
   */
  public String getCrossSiteZNode() {
    return crossSiteZNode;
  }

  /**
   * @return the root znode for clusters.
   */
  public String getClustersZNode() {
    return clustersZNode;
  }

  /**
   * Gets the znode path of the cross site tables.
   * 
   * @return
   */
  public String getTablesZNode() {
    return tablesZNode;
  }

  /**
   * Gets the znode path of the hierarchy.
   * 
   * @return
   */
  public String getHierarchyZNode() {
    return hierarchyZNode;
  }

  /**
   * Creates the znode for the cluster.
   * 
   * @param clusterInfo
   * @throws KeeperException
   */
  public void createClusterZNode(ClusterInfo clusterInfo) throws KeeperException {
    ZKUtil.createWithParents(zkw, getClusterZNode(clusterInfo.getName()));
    ZKUtil.createWithParents(zkw, getClusterAddressZNode(clusterInfo.getName()),
        Bytes.toBytes(clusterInfo.getAddress()));
    String peersZNode = getClusterPeersZNode(clusterInfo.getName());
    ZKUtil.createWithParents(zkw, peersZNode);
    if (clusterInfo.getPeers() != null) {
      for (ClusterInfo peer : clusterInfo.getPeers()) {
        ZKUtil.createWithParents(zkw, ZKUtil.joinZNode(peersZNode, peer.getName()),
            Bytes.toBytes(peer.getAddress()));
      }
    }
  }

  /**
   * Creates the znode for the cluster.
   * 
   * @param name
   * @param address
   * @throws KeeperException
   */
  public void createClusterZNode(String name, String address) throws KeeperException {
    ZKUtil.createWithParents(zkw, getClusterZNode(name));
    ZKUtil.createWithParents(zkw, getClusterAddressZNode(name), Bytes.toBytes(address));
  }

  /**
   * Creates the peer.
   * 
   * @param clusterName
   * @param peer
   * @throws KeeperException
   */
  public void createPeer(String clusterName, Pair<String, String> peer) throws KeeperException {
    String peersZNode = getClusterPeersZNode(clusterName);
    ZKUtil.createWithParents(zkw, ZKUtil.joinZNode(peersZNode, peer.getFirst()),
        Bytes.toBytes(peer.getSecond()));
  }

  /**
   * Deletes all the children nodes of the cluster but itself.
   * 
   * @param clusterName
   * @throws KeeperException
   */
  public void deletePeers(String clusterName) throws KeeperException {
    ZKUtil.deleteChildrenRecursively(zkw, getClusterPeersZNode(clusterName));
  }

  /**
   * Deletes the peers and all their children.
   * 
   * @param clusterName
   * @param peers
   * @throws KeeperException
   */
  public void deletePeers(String clusterName, String[] peers) throws KeeperException {
    String peersZNode = getClusterPeersZNode(clusterName);
    if (peers != null && peers.length > 0) {
      for (String peer : peers) {
        ZKUtil.deleteNodeRecursively(zkw, ZKUtil.joinZNode(peersZNode, peer));
      }
    } else {
      this.deletePeers(clusterName);
    }
  }

  /**
   * Deletes the znode of the cluster.
   * 
   * @param clusterName
   * @throws KeeperException
   */
  public void deleteClusterZNode(String clusterName) throws KeeperException {
    ZKUtil.deleteNodeRecursively(zkw, getClusterZNode(clusterName));
  }

  /**
   * Creates the hierarchy znode.
   * 
   * @throws KeeperException
   */
  public void createHierarchyZNode() throws KeeperException {
    ZKUtil.createWithParents(zkw, hierarchyZNode);
  }

  /**
   * Creates the hierarchy.
   * 
   * <pre>
   * Parent | --child1 | --child2
   * </pre>
   * 
   * @param parent
   * @param children
   * @throws KeeperException
   */
  public void createHierarchy(String parent, String[] children) throws KeeperException {
    String parentZNode = ZKUtil.joinZNode(hierarchyZNode, parent);
    ZKUtil.createWithParents(zkw, parentZNode);
    for (String child : children) {
      ZKUtil.createWithParents(zkw, ZKUtil.joinZNode(parentZNode, child));
    }
  }

  /**
   * Deletes the hierarchy under the parent including itself.
   * 
   * @param parent
   * @throws KeeperException
   */
  public void deleteHierarchy(String parent) throws KeeperException {
    String parentZNode = ZKUtil.joinZNode(hierarchyZNode, parent);
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, parentZNode);
    ZKUtil.deleteNodeRecursively(zkw, ZKUtil.joinZNode(hierarchyZNode, parent));
    if (children != null) {
      for (String child : children) {
        // since the child may be the parent itself, we need to add this check to avoid a infinite
        // loop.
        if (!child.equals(parent)) {
          deleteHierarchy(child);
        }
      }
    }
  }

  /**
   * Deletes the hierarchy of the children, and delete the hierarchy between the parent and
   * children.
   * 
   * @param parent
   * @param children
   * @throws KeeperException
   */
  public void deleteHierarchy(String parent, String[] children) throws KeeperException {
    if (children != null && children.length > 0) {
      String parentZNode = ZKUtil.joinZNode(hierarchyZNode, parent);
      for (String child : children) {
        ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(parentZNode, child));
        // since the child may be the parent itself, we need to add this check to avoid a infinite
        // loop.
        if (!child.equals(parent)) {
          deleteHierarchy(child);
        }
      }
    } else {
      deleteHierarchy(parent);
    }
  }

  /**
   * Lists the addresses of the clusters.
   * 
   * @return
   * @throws KeeperException
   */
  public Map<String, String> listClusterAddresses() throws KeeperException {
    Map<String, String> clusterMap = new TreeMap<String, String>();
    List<String> clusters = ZKUtil.listChildrenNoWatch(this.zkw, this.clustersZNode);
    if (clusters != null) {
      for (String cluster : clusters) {
        clusterMap.put(cluster,
            Bytes.toString(ZKUtil.getData(this.zkw, getClusterAddressZNode(cluster))));
      }
    }
    return clusterMap;
  }

  /**
   * Lists the cluster information.
   * 
   * @return
   * @throws KeeperException
   */
  public Map<String, ClusterInfo> listClusterInfos() throws KeeperException {
    Map<String, ClusterInfo> clusterMap = new TreeMap<String, ClusterInfo>();
    List<String> clusters = ZKUtil.listChildrenNoWatch(this.zkw, this.clustersZNode);
    if (clusters != null) {
      for (String cluster : clusters) {
        String address = Bytes.toString(ZKUtil.getData(this.zkw, getClusterAddressZNode(cluster)));
        List<ClusterInfo> peers = getPeerClusters(cluster);
        Set<ClusterInfo> peerSet = new TreeSet<ClusterInfo>();
        if (peers != null) {
          peerSet.addAll(peers);
        }
        ClusterInfo ci = new ClusterInfo(cluster, address, peerSet);
        clusterMap.put(cluster, ci);
      }
    }
    return clusterMap;
  }
  
  /**
   * Returns the clusterInfo for the given clusterName
   * @param clusterName
   * @return
   * @throws KeeperException
   */
  public ClusterInfo getClusterInfo(String clusterName) throws KeeperException {
    List<String> clusters = ZKUtil.listChildrenNoWatch(this.zkw, this.clustersZNode);
    if (clusters != null) {
      for (String cluster : clusters) {
        if (cluster.equals(clusterName)) {
          String address = Bytes
              .toString(ZKUtil.getData(this.zkw, getClusterAddressZNode(cluster)));
          if (address != null) {
            List<ClusterInfo> peers = getPeerClusters(cluster);
            Set<ClusterInfo> peerSet = new TreeSet<ClusterInfo>();
            if (peers != null) {
              peerSet.addAll(peers);
            }
            ClusterInfo ci = new ClusterInfo(cluster, address, peerSet);
            return ci;
          } else {
            return null;
          }
        }
      }
    }
    return null;
  }

  /**
   * Lists the peers of the cluster.
   * 
   * @param clusterName
   * @return
   * @throws KeeperException
   */
  public List<String> listClusterPeers(String clusterName) throws KeeperException {
    List<String> emptyResult = Collections.emptyList();
    List<String> result = ZKUtil.listChildrenNoWatch(this.zkw, getClusterPeersZNode(clusterName));
    return result == null ? emptyResult : result;
  }

  /**
   * Gets the znode path of the cluster address.
   * 
   * @param clusterName
   * @return
   */
  public String getClusterAddressZNode(String clusterName) {
    return ZKUtil.joinZNode(getClusterZNode(clusterName), CLUSTER_ADDRESS_ZNODE_NAME);
  }

  /**
   * Gets the znode path of the cluster peers.
   * 
   * @param clusterName
   * @return
   */
  public String getClusterPeersZNode(String clusterName) {
    return ZKUtil.joinZNode(getClusterZNode(clusterName), CLUSTER_PEERS_ZNODE_NAME);
  }

  /**
   * Gets the peer znode path of cluster.
   * 
   * @param clusterName
   * @param peer
   * @return
   */
  public String getClusterPeerZNode(String clusterName, String peer) {
    return ZKUtil.joinZNode(getClusterPeersZNode(clusterName), peer);
  }

  /**
   * Gets the znode path of the cluster.
   * 
   * @param clusterName
   * @return
   */
  public String getClusterZNode(String clusterName) {
    return ZKUtil.joinZNode(this.clustersZNode, clusterName);
  }

  /**
   * Gets all the peer clusters for the given cluster. Will return null when no peer available for
   * the given cluster
   * 
   * @param clusterName
   * @return List of peer clusters
   * @throws KeeperException
   */
  public List<ClusterInfo> getPeerClusters(String clusterName) throws KeeperException {
    List<String> peers = ZKUtil.listChildrenNoWatch(zkw, getClusterPeersZNode(clusterName));
    List<ClusterInfo> cis = Collections.emptyList();
    if (peers != null) {
      cis = new ArrayList<ClusterInfo>();
      for (String peer : peers) {
        byte[] data = ZKUtil.getData(zkw, getClusterPeerZNode(clusterName, peer));
        if (data != null) {
          ClusterInfo ci = new ClusterInfo(peer, Bytes.toString(data), (ClusterInfo[]) null);
          cis.add(ci);
        }
      }
    }
    return cis;
  }

  /**
   * Gets the peers of the cluster.
   * 
   * @param clusterName
   * @param peers
   * @return
   * @throws KeeperException
   */
  public List<ClusterInfo> getClusterPeers(String clusterName, String[] peers)
      throws KeeperException {
    if (peers != null && peers.length > 0) {
      List<ClusterInfo> cis = new ArrayList<ClusterInfo>();
      for (String peer : peers) {
        byte[] data = ZKUtil.getData(zkw, getClusterPeerZNode(clusterName, peer));
        if (data != null) {
          ClusterInfo ci = new ClusterInfo(peer, Bytes.toString(data), (ClusterInfo[]) null);
          cis.add(ci);
        }
      }
      return cis;
    } else {
      return this.getPeerClusters(clusterName);
    }
  }

  /**
   * Sets the table state in zookeeper.
   * 
   * @param tableName
   * @param state
   * @throws KeeperException
   */
  public void setTableState(String tableName, TableState state) throws KeeperException {
    ZKUtil.setData(this.zkw, getTableStateZNode(tableName), Bytes.toBytes(state.toString()));
  }

  /**
   * @param tableName
   * @return The state of the table stored in zookeeper.
   * @throws KeeperException
   * @throws TableNotFoundException
   */
  public TableState getTableState(String tableName) throws KeeperException, TableNotFoundException {
    byte[] data = ZKUtil.getData(this.zkw, getTableStateZNode(tableName));
    if (data == null) {
      throw new TableNotFoundException(tableName);
    }
    String str = Bytes.toString(data);
    try {
      return TableState.valueOf(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str);
    }
  }

  /**
   * @param tableName
   * @return The state of the table stored in zookeeper.
   * @throws KeeperException
   */
  public TableState getTableStateAllowNull(String tableName) throws KeeperException {
    byte[] data = ZKUtil.getData(this.zkw, getTableStateZNode(tableName));
    if (data == null) {
      return null;
    }
    String str = Bytes.toString(data);
    try {
      return TableState.valueOf(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str);
    }
  }

  /**
   * Finds all the state of the cross site tables.
   * 
   * @return
   * @throws KeeperException
   * @throws TableNotFoundException
   */
  public Map<String, TableState> listTableStates() throws KeeperException, TableNotFoundException {
    Map<String, TableState> states = Collections.emptyMap();
    List<String> tableNames = this.listTables();
    if (!tableNames.isEmpty()) {
      states = new HashMap<String, TableState>();
      for (String tableName : tableNames) {
        states.put(tableName, getTableState(tableName));
      }
    }
    return states;
  }

  /**
   * Check whether table exists or not
   * 
   * @param tableName
   * @return true if table exists.
   * @throws KeeperException
   */
  public boolean isTableStateExist(String tableName) throws KeeperException {
    try {
      this.getTableState(tableName);
    } catch (TableNotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Gets the znode path of the table state.
   * 
   * @param tableName
   * @return
   */
  public String getTableStateZNode(String tableName) {
    return ZKUtil.joinZNode(getTableZNode(tableName), TABLE_STATE_ZNODE_NAME);
  }

  /**
   * Locks the table by creating an ephemeral znode for a table.
   * 
   * @param tableName
   * @return
   * @throws KeeperException
   */
  public boolean lockTable(String tableName) throws KeeperException {
    boolean locked = ZKUtil.createEphemeralNodeAndWatch(zkw,
        ZKUtil.joinZNode(crossSiteZNode, tableName + EPHEMERAL), null);
    if (LOG.isDebugEnabled()) {
      LOG.debug(locked ? "Locked" : "Could not lock" + " the table " + tableName);
    }
    return locked;
  }

  /**
   * Unlocks the table by deleting the ephemeral znode.
   * 
   * @param tableName
   */
  public void unlockTable(String tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unlocking the table " + tableName);
    }
    try {
      ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(crossSiteZNode, tableName + EPHEMERAL));
    } catch (KeeperException e) {
      LOG.warn("Fail to unlock the table " + tableName, e);
    }
  }

  /**
   * Locks the table by creating an ephemeral znode for a cluster.
   * 
   * @param clusterName
   * @return
   * @throws KeeperException
   */
  public boolean lockCluster(String clusterName) throws KeeperException {
    boolean locked = ZKUtil.createEphemeralNodeAndWatch(zkw,
        ZKUtil.joinZNode(crossSiteZNode, clusterName + CLUSTER_EPHEMERAL), null);
    if (LOG.isDebugEnabled()) {
      LOG.debug(locked ? "Locked" : "Could not lock" + " the cluster " + clusterName);
    }
    return locked;
  }

  /**
   * Unlocks the cluster by deleting the ephemeral znode.
   * 
   * @param clusterName
   */
  public void unlockCluster(String clusterName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unlocking the cluster " + clusterName);
    }
    try {
      ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(crossSiteZNode, clusterName + CLUSTER_EPHEMERAL));
    } catch (KeeperException e) {
      LOG.warn("Fail to unlock the cluster " + clusterName, e);
    }
  }

  /**
   * Gets the znode path of a table.
   * 
   * @param tableName
   * @return
   */
  public String getTableZNode(String tableName) {
    return ZKUtil.joinZNode(this.tablesZNode, tableName);
  }

  /**
   * Gets the znode path of the table descriptor.
   * 
   * @param tableName
   * @return
   */
  public String getTableDescZNode(String tableName) {
    return ZKUtil.joinZNode(getTableZNode(tableName), TABLE_DESC_ZNODE_NAME);
  }

  /**
   * Gets the znode path of the split keys.
   * 
   * @param tableName
   * @return
   */
  public String getTableSplitKeysZNode(String tableName) {
    return ZKUtil.joinZNode(getTableZNode(tableName), TABLE_SPLITKEYS_ZNODE_NAME);
  }

  /**
   * @param tableName
   * @return Initial split keys with which given table was created. Null when the table was created
   *         with out specifying any splits.
   * @throws KeeperException
   * @throws IOException
   */
  public byte[][] getTableSplitKeys(String tableName) throws KeeperException, IOException {
    byte[] data = ZKUtil.getData(zkw, getTableSplitKeysZNode(tableName));
    return CrossSiteUtil.readSplitKeys(data);
  }

  /**
   * Gets the znode path of the cluster locator.
   * 
   * @param tableName
   * @return
   */
  public String getClusterLocatorZNode(String tableName) {
    return ZKUtil.joinZNode(getTableZNode(tableName), CLUSTER_LOCATOR_ZNODE_NAME);
  }

  /**
   * Creates the znode of a table.
   * 
   * @param tableName
   * @throws KeeperException
   */
  public void createTableZNode(String tableName) throws KeeperException {
    ZKUtil.createWithParents(this.zkw, getTableZNode(tableName));
  }

  /**
   * Deletes the znode of a table.
   * 
   * @param tableName
   * @throws KeeperException
   */
  public void deleteTableZNode(String tableName) throws KeeperException {
    ZKUtil.deleteNodeRecursively(this.zkw, getTableZNode(tableName));
  }

  /**
   * Modify the znode data of the table descriptor.
   * 
   * @param tableName
   * @param desc
   * @throws KeeperException
   * @throws IOException
   */
  public void modifyTableDesc(String tableName, HTableDescriptor desc) throws KeeperException,
      IOException {
    // Do table existence check
    getTableState(tableName);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(stream);
    desc.write(out);
    ZKUtil.setData(this.zkw, getTableDescZNode(tableName), stream.toByteArray());
    stream.close();
  }

  /**
   * Write the znode data of the new proposed table descriptor.
   * 
   * @param tableName
   * @param desc
   * @throws KeeperException
   * @throws IOException
   */
  public void writeProposedTableDesc(String tableName, HTableDescriptor desc)
      throws KeeperException, IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(stream);
    desc.write(out);
    // The node should be created here
    ZKUtil.createSetData(this.zkw,
        ZKUtil.joinZNode(getTableZNode(tableName), TABLE_PROPOSED_DESC_ZNODE_NAME),
        stream.toByteArray());
    stream.close();
  }

  /**
   * Deletes the proposed HTD node from zk.
   * 
   * @param tableName
   * @throws KeeperException
   * @throws IOException
   */
  public void deleteProposedTableDesc(String tableName) throws KeeperException, IOException {
    ZKUtil.deleteNode(zkw,
        ZKUtil.joinZNode(getTableZNode(tableName), TABLE_PROPOSED_DESC_ZNODE_NAME));
  }

  /**
   * Gets proposed HTD written to zk.
   * 
   * @param tableName
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor getProposedTableDesc(String tableName) throws KeeperException,
      IOException {
    byte[] data = ZKUtil.getData(zkw,
        ZKUtil.joinZNode(getTableZNode(tableName), TABLE_PROPOSED_DESC_ZNODE_NAME));
    if(data == null) return null;
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInput in = new DataInputStream(stream);
    HTableDescriptor htd = new HTableDescriptor();
    htd.readFields(in);
    stream.close();
    return htd;
  }

  /**
   * Gets the table descriptor for the table descriptor znode.
   * 
   * @param tableName
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor getTableDesc(String tableName) throws KeeperException, IOException {
    // Do table existence check
    getTableState(tableName);
    byte[] data = ZKUtil.getData(this.zkw, getTableDescZNode(tableName));
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInput in = new DataInputStream(stream);
    HTableDescriptor htd = new HTableDescriptor();
    htd.readFields(in);
    stream.close();
    return htd;
  }

  /**
   * Gets the table descriptor for the table descriptor znode.
   * 
   * @param tableName
   * @return null if the table is not existent
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor getTableDescAllowNull(String tableName) throws KeeperException, IOException {
    // Do table existence check
    byte[] stateData = ZKUtil.getData(this.zkw, getTableStateZNode(tableName));
    if (stateData == null) {
      return null;
    }
    byte[] data = ZKUtil.getData(this.zkw, getTableDescZNode(tableName));
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInput in = new DataInputStream(stream);
    HTableDescriptor htd = new HTableDescriptor();
    htd.readFields(in);
    stream.close();
    return htd;
  }

  /**
   * Lists the descriptors of all the cross site tables.
   * 
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor[] listTableDescs() throws KeeperException, IOException {
    return listTableDescs((Pattern) null);
  }

  /**
   * Lists the descriptors of all the cross site tables with the pattern.
   * 
   * @param pattern
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor[] listTableDescs(Pattern pattern) throws KeeperException, IOException {
    List<HTableDescriptor> htds = Collections.emptyList();
    List<String> tableNames = listTables();
    if (tableNames != null && !tableNames.isEmpty()) {
      htds = new ArrayList<HTableDescriptor>();
      for (String tableName : tableNames) {
        if (pattern == null || pattern.matcher(tableName).matches()) {
          byte[] data = ZKUtil.getData(this.zkw, getTableDescZNode(tableName));
          if (data != null) {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            DataInput in = new DataInputStream(stream);
            HTableDescriptor htd = new HTableDescriptor();
            htd.readFields(in);
            stream.close();
            htds.add(htd);
          }
        }
      }
    }
    return htds.toArray(new HTableDescriptor[0]);
  }

  /**
   * Lists the descriptors of the cross site tables.
   * 
   * @param tableNames
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public HTableDescriptor[] listTableDescs(List<String> tableNames) throws KeeperException,
      IOException {
    List<HTableDescriptor> htds = Collections.emptyList();
    if (tableNames != null && !tableNames.isEmpty()) {
      htds = new ArrayList<HTableDescriptor>();
      for (String tableName : tableNames) {
        byte[] data = ZKUtil.getData(this.zkw, getTableDescZNode(tableName));
        if (data != null) {
          ByteArrayInputStream stream = new ByteArrayInputStream(data);
          DataInput in = new DataInputStream(stream);
          HTableDescriptor htd = new HTableDescriptor();
          htd.readFields(in);
          stream.close();
          htds.add(htd);
        }
      }
    }
    return htds.toArray(new HTableDescriptor[0]);
  }

  /**
   * Lists all the cross site tables.
   * 
   * @return
   * @throws KeeperException
   */
  public List<String> listTables() throws KeeperException {
    List<String> emptyResult = Collections.emptyList();
    List<String> result = ZKUtil.listChildrenNoWatch(zkw, tablesZNode);
    return result == null ? emptyResult : result;
  }

  /**
   * Gets all the cross site tables.
   * 
   * @return an array of the table names.
   * @throws KeeperException
   */
  public String[] getTableNames() throws KeeperException {
    List<String> tableNames = ZKUtil.listChildrenNoWatch(zkw, this.tablesZNode);
    String[] emptyArray = new String[0];
    return tableNames == null ? emptyArray : tableNames.toArray(emptyArray);
  }

  /**
   * Gets the table names of all the cross site tables with the pattern.
   * 
   * @param pattern
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public String[] getTableNames(Pattern pattern) throws KeeperException, IOException {
    List<String> results = Collections.emptyList();
    List<String> tableNames = ZKUtil.listChildrenNoWatch(zkw, this.tablesZNode);
    if (tableNames != null && !tableNames.isEmpty()) {
      results = new ArrayList<String>();
      for (String tableName : tableNames) {
        if (pattern.matcher(tableName).matches()) {
          results.add(tableName);
        }
      }
    }
    return results.toArray(new String[0]);
  }

  /**
   * Gets the znode data of the cluster locator.
   * 
   * @param tableName
   * @return
   * @throws KeeperException
   * @throws IOException
   */
  public byte[] getClusterLocatorData(String tableName) throws KeeperException, IOException {
    // Do table existence check
    getTableState(tableName);
    return ZKUtil.getData(this.zkw, getClusterLocatorZNode(tableName));
  }

  /**
   * Gets a cluster locator.
   * 
   * @param tableName
   * @return
   * @throws IOException
   * @throws KeeperException
   */
  public ClusterLocator getClusterLocator(String tableName) throws IOException, KeeperException {
    byte[] clusterLocatorData = getClusterLocatorData(tableName);
    ByteArrayInputStream stream = null;
    try {
      stream = new ByteArrayInputStream(clusterLocatorData);
      DataInput in = new DataInputStream(stream);
      ClusterLocatorRPCObject locator = new ClusterLocatorRPCObject();
      locator.readFields(in);
      return locator.getClusterLocator();
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.warn("Fail to close the stream of reading cluster locator", e);
        }
      }
    }
  }

  /**
   * Checks whether the cluster is existent.
   * 
   * @param clusterName
   * @return
   * @throws KeeperException
   */
  public boolean clusterExists(String clusterName) throws KeeperException {
    return ZKUtil.checkExists(this.zkw, getClusterZNode(clusterName)) >= 0;
  }

  /**
   * Gets the cluster address.
   * 
   * @param clusterName
   * @return
   * @throws KeeperException
   */
  public String getClusterAddress(String clusterName) throws KeeperException {
    if (!clusterExists(clusterName)) {
      throw new IllegalArgumentException("The cluster " + clusterName + " is not found");
    }
    byte[] data = ZKUtil.getData(this.zkw, getClusterAddressZNode(clusterName));
    return Bytes.toString(data);
  }

  /**
   * Lists the clusters.
   * 
   * @return
   * @throws KeeperException
   */
  public List<String> listClusters() throws KeeperException {
    List<String> empty = Collections.emptyList();
    List<String> result = ZKUtil.listChildrenNoWatch(this.zkw, this.clustersZNode);
    return result == null ? empty : result;
  }

  /**
   * Gets the map of the hierarchy.
   * 
   * @return
   * @throws KeeperException
   */
  public Map<String, Set<String>> getHierarchyMap() throws KeeperException {
    Map<String, Set<String>> hierarchy = new TreeMap<String, Set<String>>();
    List<String> parents = ZKUtil.listChildrenNoWatch(this.zkw, this.hierarchyZNode);
    if (parents != null) {
      for (String parent : parents) {
        List<String> children = ZKUtil.listChildrenNoWatch(this.zkw,
            ZKUtil.joinZNode(this.hierarchyZNode, parent));
        if (children != null) {
          TreeSet<String> childrenSet = new TreeSet<String>();
          childrenSet.addAll(children);
          hierarchy.put(parent, childrenSet);
        }
      }
    }
    return hierarchy;
  }

  /**
   * Checks whether the table is existent.
   * 
   * @param tableName
   * @return true if this table is existent.
   * @throws KeeperException
   */
  public boolean isTableExists(String tableName) throws KeeperException {
    return (ZKUtil.checkExists(zkw, getTableZNode(tableName)) >= 0);
  }

  /**
   * Checks whether the peer is existent.
   * 
   * @param peer
   * @return
   * @throws KeeperException
   */
  public boolean isPeerExists(String clusterName, String peer) throws KeeperException {
    return (ZKUtil.checkExists(zkw, ZKUtil.joinZNode(getClusterPeersZNode(clusterName), peer)) >= 0);
  }
}

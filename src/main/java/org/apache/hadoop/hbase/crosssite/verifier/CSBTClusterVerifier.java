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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.CrossSiteCallable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteDummyAbortable;
import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes;
import org.apache.hadoop.hbase.crosssite.CrossSiteZNodes.TableState;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

/**
 * This is a tool that verifies the status of the CSBT cluster The following
 * things would be taken care by this tool and additions may be done to this in
 * future 
 * 1) Verifies the tables in the cluster. All the tables should exist as
 * in the crosssite tableznode 
 * 2) Verifies if the HTD in the actual tables and
 * the one in the crosssite znodes are same 
 * 3) If replication is enabled check
 * if the peers have the required tables with the required CFs
 */
public class CSBTClusterVerifier extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CSBTClusterVerifier.class);
  private ExecutorService executor;
  private final static int MAX_NUM_THREADS = 50;
  private int retCode = 0;
  private boolean fixTables = false;
  private boolean fixTableStates = false;
  private boolean fixHTDs = false;
  private CrossSiteZNodes crossSiteZnodes;

  public CSBTClusterVerifier(Configuration conf) {
    super(conf);
    int numThreads = conf.getInt("hbase.crosssite.verifier.numthreads", MAX_NUM_THREADS);
    executor = new ScheduledThreadPoolExecutor(numThreads);
  }
  
  public CSBTClusterVerifier(Configuration conf, ExecutorService executor) {
    super(conf);
    this.executor = executor;
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Path hbasedir = new Path(conf.get(HConstants.HBASE_DIR));
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    conf.set("fs.defaultFS", defaultFs.toString()); // for hadoop 0.21+
    conf.set("fs.default.name", defaultFs.toString()); // for hadoop 0.20
    int ret = ToolRunner.run(new CSBTClusterVerifier(conf), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    exec(executor, args);
    return getReturnCode();
  }

  int getReturnCode() {
    return retCode;
  }

  void setReturnCode(int retCode) {
    this.retCode = retCode;
  }

  void exec(ExecutorService executor, String[] args) throws IOException, KeeperException {
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-createTables")) {
        fixTables(true);
      } else if (cmd.equals("-fixTableStates")) {
        fixTableStates(true);
      } else if (cmd.equals("-fixHTDs")) {
        fixHTDs(true);
      }
    }
    // Create connection with CrossSiteHBaseAdmin
    connect();
    onlineVerification(executor);
  }

  void connect() throws IOException, KeeperException {
    ZooKeeperWatcher zooKeeperWatcher = new ZooKeeperWatcher(getConf(),
        "connection to global zookeeper from CSBTClusterVerifier ", new CrossSiteDummyAbortable(),
        false);
    crossSiteZnodes = new CrossSiteZNodes(zooKeeperWatcher);
  }

  void onlineVerification(ExecutorService executor) throws IOException, KeeperException {
    // Step 1 : Verify the clusters and the table znodes.
    // Ensure that the clusters have all the tables created as specified in the
    // table znode
    verifyClusterAndTables();

    // Step 2 : Verify the HTDs. If there is a mismatch make the appropriate
    // step to make the HTDs in the crosssite
    // znode and the actual HTD of the tables in the cluster to be in sync
    verifyHTDs();
    
    //Step 3: Verify the states of the tables in the cluster and the peers
    // Currently will not do rectification here for all cases because if state is in DISABLING/ENABLING
    // we cannot do the correction easily.  If the state is DISABLED instead of ENABLED, or if
    // the state is ENABLED instead of DISABLED those can be corrected
    verifyTableStatesInClusterAndPeers();
    
  }

   void verifyTableStatesInClusterAndPeers() throws KeeperException, IOException {
    LOG.debug("Collecting the table state in the cluster and the peers");
    final Map<String, ClusterInfo> clusterInfos = crossSiteZnodes.listClusterInfos();
    try {
      if (!clusterInfos.isEmpty()) {
        final HTableDescriptor[] tableDescsFromZnode = crossSiteZnodes.listTableDescs();
        final Map<String, TableState> tableStates = crossSiteZnodes.listTableStates();
        for (Iterator<Entry<String, TableState>> tableStatesIr = tableStates.entrySet().iterator(); tableStatesIr
            .hasNext();) {
          Entry<String, TableState> tableStateEntry = tableStatesIr.next();
          TableState state = tableStateEntry.getValue();
          if (state != TableState.ENABLED && state != TableState.DISABLED) {
            LOG.error("Table " + tableStateEntry.getKey() + " in abnormal state["
                + state.toString() + "]. Cannot handle through this tool.");
            tableStatesIr.remove();
            setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
          }
        }
        if (tableStates.isEmpty()) {
          return;
        }
        List<Future<Map<String, Map<String, TableState>>>> results = new ArrayList<Future<Map<String, Map<String, TableState>>>>();
        Map<String, Map<String, TableState>> workingMap = new HashMap<String, Map<String, TableState>>();
        for (final Entry<String, ClusterInfo> entry : clusterInfos.entrySet()) {
          results.add(executor.submit(new CrossSiteCallable<Map<String, Map<String, TableState>>>(
              getConf()) {

            @Override
            public Map<String, Map<String, TableState>> call() throws Exception {
              Map<String, Map<String, TableState>> clusterTableStates = Collections.emptyMap();
              if (tableDescsFromZnode != null && tableDescsFromZnode.length > 0) {
                clusterTableStates = new HashMap<String, Map<String, TableState>>();
                HBaseAdmin admin = createHBaseAdmin(configuration, entry.getValue().getAddress());
                try {
                  Map<String, TableState> states = new HashMap<String, TableState>();
                  clusterTableStates.put(entry.getValue().getAddress(), states);
                  for (HTableDescriptor htd : tableDescsFromZnode) {
                    boolean peerShouldEnabled = false;
                    String tableName = Bytes.toString(htd.getName());
                    TableState state = tableStates.get(tableName);
                    String clusterTableName = CrossSiteUtil.getClusterTableName(tableName,
                        entry.getKey());
                    if (state == TableState.ENABLED) {
                      peerShouldEnabled = true;
                      if (admin.tableExists(clusterTableName)
                          && !admin.isTableEnabled(clusterTableName)) {
                        states.put(clusterTableName, TableState.ENABLED);
                        LOG.error("The state of the table " + clusterTableName + " in the cluster "
                            + entry.getKey() + " is disabled, should be corrected to the enabled");
                      }
                    } else if (state == TableState.DISABLED) {
                      if (admin.tableExists(clusterTableName)
                          && !admin.isTableDisabled(clusterTableName)) {
                        states.put(clusterTableName, TableState.DISABLED);
                        LOG.error("The state of the table " + clusterTableName + " in the cluster "
                            + entry.getKey() + " is enabled, should be corrected to the disabled");
                      }
                    } else {
                      LOG.error("Table " + tableName + " in abnormal state["
                          + (state == null ? "" : state.toString())
                          + "]. Cannot handle through this tool.");
                      setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
                    }
                    // If the primary table is enabled, we must guarantee the peer tables are
                    // enabled.
                    // if the primary table is disabled, we disregards the states of the peer
                    // tables.
                    if (peerShouldEnabled) {
                      boolean createTableInPeers = ClusterVerifierUtil.isReplicatedTable(htd);
                      if (createTableInPeers) {
                        ClusterInfo ci = entry.getValue();
                        if (ci.getPeers() != null) {
                          for (ClusterInfo peer : ci.getPeers()) {
                            HBaseAdmin peerAdmin = ClusterVerifierUtil.createHBaseAmin(
                                configuration, peer.getAddress());
                            try {
                              String peerTableName = CrossSiteUtil.getPeerClusterTableName(
                                  tableName, ci.getName(), peer.getName());
                              if (peerAdmin.tableExists(peerTableName)) {
                                if (!peerAdmin.isTableEnabled(peerTableName)) {
                                  Map<String, TableState> peerStates = clusterTableStates.get(peer
                                      .getAddress());
                                  if (peerStates == null) {
                                    peerStates = new HashMap<String, TableState>();
                                    clusterTableStates.put(peer.getAddress(), peerStates);
                                  }
                                  peerStates.put(peerTableName, TableState.ENABLED);
                                  LOG.error("The state of the peer table " + peerTableName
                                      + " in the cluster " + peer.getName()
                                      + " is disabled, should be corrected to the enabled");
                                }
                              }
                            } finally {
                              closeHBaseAdmin(peerAdmin);
                            }
                          }
                        }
                      }
                    }
                  }
                } finally {
                  closeHBaseAdmin(admin);
                }
              }
              return clusterTableStates;
            }
          }));
        }
        for (Future<Map<String, Map<String, TableState>>> result : results) {
          Map<String, Map<String, TableState>> partialResult = result.get();
          for (Entry<String, Map<String, TableState>> partialResultEntry : partialResult.entrySet()) {
            Map<String, TableState> tableStateMap = workingMap.get(partialResultEntry.getKey());
            if (tableStateMap == null) {
              workingMap.put(partialResultEntry.getKey(), partialResultEntry.getValue());
            } else {
              tableStateMap.putAll(partialResultEntry.getValue());
            }
          }
        }

        // This tries to rectiy if the table is not in enabled or disabled state
        LOG.debug("Verifying the results");
        if (!workingMap.isEmpty()) {
          if (shouldFixTableStates()) {
            setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
            for (Entry<String, Map<String, TableState>> workingEntry : workingMap.entrySet()) {
              HBaseAdmin admin = createHBaseAdmin(getConf(), workingEntry.getKey());
              try {
                for (Entry<String, TableState> tableStateEntry : workingEntry.getValue().entrySet()) {
                  String tableName = tableStateEntry.getKey();
                  TableState state = tableStateEntry.getValue();
                  if (state == TableState.ENABLED) {
                    ClusterVerifierUtil.enableTable(admin, tableName);
                  } else {
                    // the state must be disabled
                    ClusterVerifierUtil.disableTable(admin, tableName);
                  }
                }
              } finally {
                closeHBaseAdmin(admin);
              }
            }
            setReturnCode(RETURN_CODE.ERROR_FIXED.ordinal());
            LOG.info("The states of the tables have been corrected");
          } else {
            for (Entry<String, Map<String, TableState>> workingEntry : workingMap.entrySet()) {
              // TODO : Create an error report as in HBCK
              if (workingEntry.getValue() != null && !workingEntry.getValue().isEmpty()) {
                String message = "The cluster " + workingEntry.getKey();
                StringBuilder tables = new StringBuilder();
                for (String tableName : workingEntry.getValue().keySet()) {
                  tables.append(tableName).append(" ");
                }
                message += " has the following tables " + tables.toString()
                    + " with inconsistent state";
                System.out.println(message);
                System.out.println();
                setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
              }
            }
          }
        } else {
          LOG.info("All the tables are in the correct state");
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while verifying table status in cluster and peer", e);
      setReturnCode(RETURN_CODE.EXCEPTION_ON_ERROR_FIX.ordinal());
      throw new IOException(e);
    }
  }

  void verifyHTDs() throws IOException, KeeperException {
    // This considers that the htd in the crosssite znodes are the truth. The
    // ones inside the cluster
    // would be modified if there are any changes
    LOG.debug("Collecting the list of clusters and the list of tables for verifying HTDs");
    Map<String, ClusterInfo> clusterInfos = crossSiteZnodes.listClusterInfos();
    final HTableDescriptor[] tableDescsFromZnode = crossSiteZnodes.listTableDescs();
    List<Future<Map<String, List<HTableDescriptor>>>> results = 
        new ArrayList<Future<Map<String, List<HTableDescriptor>>>>();
    for (final Entry<String, ClusterInfo> entry : clusterInfos.entrySet()) {
      results.add(executor.submit(new CrossSiteCallable<Map<String, List<HTableDescriptor>>>(getConf()) {
        @Override
        public Map<String, List<HTableDescriptor>> call() throws Exception {
          Map<String, List<HTableDescriptor>> results = 
              new HashMap<String, List<HTableDescriptor>>();
          String clusterName = entry.getKey();
          ClusterInfo clusterInfo = entry.getValue();
          HBaseAdmin admin = createHBaseAdmin(configuration, entry.getValue().getAddress());
          HTableDescriptor[] listTables = null;
          try {
            listTables = admin.listTables();
          } finally {
            closeHBaseAdmin(admin);
          }
          for (HTableDescriptor tableDescFromZNode : tableDescsFromZnode) {
            String clusterTableName = CrossSiteUtil.getClusterTableName(
                Bytes.toString(tableDescFromZNode.getName()), clusterName);
            boolean found = false;
            HTableDescriptor tempHTD = null;
            for (HTableDescriptor tableDesc : listTables) {
              if (clusterTableName.equals(Bytes.toString(tableDesc.getName()))) {
                found = true;
                tempHTD = tableDesc;
                break;
              }
            }
            if (found) {
              HTableDescriptor clonedDescFromZNode = new HTableDescriptor(tableDescFromZNode);
              clonedDescFromZNode.setName(Bytes.toBytes(clusterTableName));
              if (!clonedDescFromZNode.equals(tempHTD)) {
                List<HTableDescriptor> issues = results.get(clusterName);
                if (issues == null) {
                  issues = new ArrayList<HTableDescriptor>();
                  results.put(clusterName, issues);
                }
                issues.add(clonedDescFromZNode);
              }
            }
            if (ClusterVerifierUtil.isReplicatedTable(tableDescFromZNode)) {
              // check the tables in peers
              if (clusterInfo.getPeers() != null) {
                for (ClusterInfo peer : clusterInfo.getPeers()) {
                  HBaseAdmin peerAdmin = createHBaseAdmin(configuration, peer.getAddress());
                  try {
                    if (peerAdmin.isTableAvailable(clusterTableName)) {
                      HTableDescriptor peerHtd = peerAdmin.getTableDescriptor(Bytes.toBytes(clusterTableName));
                      HTableDescriptor clonedDescFromZNode = new HTableDescriptor(tableDescFromZNode);
                      clonedDescFromZNode.setName(Bytes.toBytes(clusterTableName));
                      for (HColumnDescriptor hcd : clonedDescFromZNode.getColumnFamilies()) {
                        if (hcd.getScope()>0) {
                          hcd.setScope(0);
                        }
                      }
                      if (!clonedDescFromZNode.equals(peerHtd)) {
                        List<HTableDescriptor> issues = results.get(clusterName);
                        if (issues == null) {
                          issues = new ArrayList<HTableDescriptor>();
                          results.put(clusterName, issues);
                        }
                        issues.add(clonedDescFromZNode);
                      }
                    }
                  } finally {
                    closeHBaseAdmin(peerAdmin);
                  }
                }
              }
            }
          }
          return results;
        }
      }));
    }
    LOG.debug("Verifying the htd results");
    try {
      Map<String, List<HTableDescriptor>> issues = new HashMap<String, List<HTableDescriptor>>();
      for (Future<Map<String, List<HTableDescriptor>>> result : results) {
        Map<String, List<HTableDescriptor>> htdMap = result.get();
        for (Entry<String, List<HTableDescriptor>> htdEntry : htdMap.entrySet()) {
          List<HTableDescriptor> issuesPerCluster = issues.get(htdEntry.getKey());
          if (issuesPerCluster == null) {
            issues.put(htdEntry.getKey(), htdEntry.getValue());
          } else {
            issuesPerCluster.addAll(htdEntry.getValue());
          }
        }
      }
      if(!issues.isEmpty()) {
        if (!shouldFixHTDs()) {
          // TODO : Create an error report as in HBCK
          String message = "The following htds do not match with the ones in the crosssite htd znode"
              + issues;
          System.out.println(message);
          System.out.println();
          setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
        } else {
          for(Entry<String, List<HTableDescriptor>> issueEntry : issues.entrySet()) {
            ClusterInfo clusterInfo = crossSiteZnodes.getClusterInfo(issueEntry.getKey());
            if (clusterInfo != null) {
              HBaseAdmin admin = createHBaseAdmin(getConf(), clusterInfo.getAddress());
              try {
                for (HTableDescriptor htd : issueEntry.getValue()) {
                  boolean enabled = false;
                  if (!admin.isTableDisabled(htd.getName())) {
                    LOG.debug("Disabling the table " + htd.getNameAsString());
                    admin.disableTable(htd.getName());
                    enabled = true;
                  }
                  LOG.debug("Modifying the table " + htd.getNameAsString());
                  admin.modifyTable(htd.getName(), htd);
                  if (enabled) {
                    LOG.debug("Enabling the table " + htd.getNameAsString());
                    admin.enableTable(htd.getName());
                  }
                }
              } finally {
                closeHBaseAdmin(admin);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while verifying HTDs", e);
      setReturnCode(RETURN_CODE.EXCEPTION_ON_ERROR_FIX.ordinal());
      throw new IOException(e);
    }
  }

  void verifyClusterAndTables() throws KeeperException, IOException {
    LOG.debug("Collecting the list of clusters and the list of tables");
    Map<String, ClusterInfo> clusterInfos = crossSiteZnodes.listClusterInfos();
    final HTableDescriptor[] tableDescsFromZnode = crossSiteZnodes.listTableDescs();
    List<Future<Map<String, Map<String, HTableDescriptor>>>> results = 
        new ArrayList<Future<Map<String, Map<String, HTableDescriptor>>>>();
    for (final Entry<String, ClusterInfo> entry : clusterInfos.entrySet()) {
      results
          .add(executor
              .submit(new CrossSiteCallable<Map<String, Map<String, HTableDescriptor>>>(
                  getConf()) {

        @Override
        public Map<String, Map<String, HTableDescriptor>> call() throws Exception {
          Map<String, Map<String, HTableDescriptor>> results = 
              new HashMap<String, Map<String, HTableDescriptor>>();
          String clusterName = entry.getKey();
          ClusterInfo clusterInfo = entry.getValue();
          HBaseAdmin admin = createHBaseAdmin(configuration, entry.getValue().getAddress());
          HTableDescriptor[] listTables = null;
          try {
            listTables = admin.listTables(); 
          } finally {
            closeHBaseAdmin(admin);
          }
          for (HTableDescriptor tableDescFromZNode : tableDescsFromZnode) {
            boolean found = false;
            String clusterTableName = CrossSiteUtil.getClusterTableName(
                Bytes.toString(tableDescFromZNode.getName()), clusterName);
            for (HTableDescriptor tableDesc : listTables) {
              if (Bytes.toString(tableDesc.getName()).equals(clusterTableName)) {
                found = true;
                break;
              }
            }
            if (!found) {
              Map<String, HTableDescriptor> notFoundTableMap = results.get(clusterName);
              if (results.get(clusterName) == null) {
                notFoundTableMap = new HashMap<String, HTableDescriptor>();
                results.put(clusterName, notFoundTableMap);
              }
              HTableDescriptor htd = new HTableDescriptor(tableDescFromZNode);
              htd.setName(Bytes.toBytes(clusterTableName));
              notFoundTableMap.put(clusterTableName, htd);
            }
            if (ClusterVerifierUtil.isReplicatedTable(tableDescFromZNode)) {
              // check the tables in peers
              if (clusterInfo.getPeers() != null) {
                for (ClusterInfo peer : clusterInfo.getPeers()) {
                  HBaseAdmin peerAdmin = createHBaseAdmin(configuration, peer.getAddress());
                  try {
                    if (!peerAdmin.isTableAvailable(clusterTableName)) {
                      Map<String, HTableDescriptor> notFoundTableMap = results.get(peer.getName());
                      if (results.get(peer.getName()) == null) {
                        notFoundTableMap = new HashMap<String, HTableDescriptor>();
                        results.put(peer.getName(), notFoundTableMap);
                      }
                      HTableDescriptor htd = new HTableDescriptor(tableDescFromZNode);
                      htd.setName(Bytes.toBytes(clusterTableName));
                      for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
                        if (hcd.getScope()>0) {
                          hcd.setScope(0);
                        }
                      }
                      notFoundTableMap.put(clusterTableName, htd);
                    }
                  } finally {
                    closeHBaseAdmin(peerAdmin);
                  }
                }
              }
            }
          }
          return results;
        }
      }));
    }
    LOG.debug("Verifying the results");
    try {
      Map<String, Map<String, HTableDescriptor>> issues = new HashMap<String, Map<String, HTableDescriptor>>();
      for (Future<Map<String, Map<String, HTableDescriptor>>> result : results) {
        Map<String, Map<String, HTableDescriptor>> htdMap = result.get();
        for (Entry<String, Map<String, HTableDescriptor>> htdEntry : htdMap.entrySet()) {
          Map<String, HTableDescriptor> issuesPerCluster = issues.get(htdEntry.getKey());
          if (issuesPerCluster == null) {
            issues.put(htdEntry.getKey(), htdEntry.getValue());
          } else {
            issuesPerCluster.putAll(htdEntry.getValue());
          }
        }
      }
      if (!issues.isEmpty()) {
        LOG.debug("Mismatch in the tables actually created in the clusters with the list of tables in the crosssite table znode:"
            + issues);
        Set<Entry<String, Map<String, HTableDescriptor>>> clusterWithIssues = issues.entrySet();
        if (!shouldFixTables()) {
          for (Entry<String, Map<String, HTableDescriptor>> entry : clusterWithIssues) {
            // TODO : Create an error report as in HBCK
            String message = "The cluster " + entry.getKey();
            StringBuilder tables = new StringBuilder();
            if (entry.getValue() != null) {
              for (String table : entry.getValue().keySet()) {
                tables.append(table).append(" ");
              }
            }
            message += " has the following missing tables " + tables.toString();
            System.out.println(message);
            System.out.println();
          }
          setReturnCode(RETURN_CODE.REPORT_ERROR.ordinal());
        } else {
          LOG.debug("Creating tables in the clusters which does not have the table as given in the table znode");
          for (Entry<String, Map<String, HTableDescriptor>> entry : clusterWithIssues) {
            Map<String, HTableDescriptor> tableDescs = entry.getValue();
            if (tableDescs != null) {
              for (Entry<String, HTableDescriptor> tableDesc : tableDescs.entrySet()) {
                String clusterName = entry.getKey();
                LOG.debug("Creating table " + tableDesc.getKey() + " in the cluster "
                    + clusterName);
                String tableName = CrossSiteUtil.getCrossSiteTableName(tableDesc.getKey());
                String actualClusterName = CrossSiteUtil.getClusterName(tableDesc.getKey());
                ClusterInfo clusterInfo = crossSiteZnodes.getClusterInfo(clusterName);
                ClusterLocator clusterLocator = crossSiteZnodes.getClusterLocator(tableName);
                byte[][] tableSplitKeys = clusterLocator.getSplitKeys(actualClusterName,
                    crossSiteZnodes.getTableSplitKeys(tableName));
                if (clusterInfo != null) {
                  HBaseAdmin admin = createHBaseAdmin(getConf(), clusterInfo.getAddress());
                  try {
                    admin.createTable(tableDesc.getValue(), tableSplitKeys);
                  } finally {
                    closeHBaseAdmin(admin);
                  }
                  LOG.debug("Created table " + tableDesc.getKey() + " in the cluster "
                      + clusterName);
                  setReturnCode(RETURN_CODE.ERROR_FIXED.ordinal());
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Exception while verifying the cluster and table results", e);
      setReturnCode(RETURN_CODE.EXCEPTION_ON_ERROR_FIX.ordinal());
      throw new IOException(e);
    }
  }

  private static HBaseAdmin createHBaseAdmin(Configuration baseConf, String clusterAddress)
      throws IOException {
    Configuration clusterConf = new Configuration(baseConf);
    ZKUtil.applyClusterKeyToConf(clusterConf, clusterAddress);
    return new HBaseAdmin(clusterConf);
  }

  private static void closeHBaseAdmin(HBaseAdmin admin) {
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.warn("Fail to close the HBaseAdmin", e);
      }
    }
  }

  void fixTables(boolean fixTables) {
    this.fixTables = fixTables;
  }

  boolean shouldFixTables() {
    return this.fixTables;
  }

  boolean shouldFixTableStates() {
    return this.fixTableStates;
  }

  private boolean shouldFixHTDs() {
    return this.fixHTDs;
  }

  void fixTableStates(boolean fixTablesInPeers) {
    this.fixTableStates = fixTablesInPeers;
  }

  void fixHTDs(boolean fixHTDs) {
    this.fixHTDs = fixHTDs;
  }

  enum RETURN_CODE {
    REPORT_ERROR, // this error code is returned when only an error is reported
                  // and there is no rectification
    ERROR_FIXED, // this error code is returned after the error is reported and
                 // fixed
    EXCEPTION_ON_ERROR_FIX // this error code is returned after there is an exception 
                           // while fixing the error

  }
}

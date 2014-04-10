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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.crosssite.ClusterInfo;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteHBaseAdminWithDelete {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();
  private CrossSiteHBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.crosssite.table.failover", true);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 100);
    TEST_UTIL.getConfiguration().setBoolean(
        CrossSiteConstants.CROSS_SITE_TABLE_SCAN_IGNORE_UNAVAILABLE_CLUSTERS, true);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL1.getConfiguration().setBoolean("hbase.crosssite.table.failover", true);
    TEST_UTIL1.getConfiguration().setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    TEST_UTIL1.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL1.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 100);
    TEST_UTIL1.startMiniCluster(1);
    TEST_UTIL1.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL2.getConfiguration().setBoolean("hbase.crosssite.table.failover", true);
    TEST_UTIL2.getConfiguration().setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    TEST_UTIL2.startMiniCluster(1);
    TEST_UTIL2.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testDeleteClusterWithExistingTables() throws Exception {
    String tableName = "testAddClusterWithExistingTables";
    HTableDescriptor desc = new HTableDescriptor("testAddClusterWithExistingTables");
    desc.addFamily(new HColumnDescriptor("col1"));
    this.admin.createTable(desc);
    // HBaseAdmin only waits for regions to appear in META we should wait until
    // they are assigned
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL, false);
    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(this.admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
    String HBASE1 = "hbase1";
    this.admin.addCluster(HBASE1, TEST_UTIL1.getClusterKey());
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL1, true);
    // Just verify that it is not created in the base cluster
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL, false);
    this.admin.deleteCluster(HBASE1);
    ClusterInfo[] infos = this.admin.listClusters();
    Assert.assertTrue(infos.length == 0);
  }

  @Test
  public void testDeletePeers() throws Exception {
    String HBASE2 = "hbase2";
    Pair<String, String> peer = new Pair<String, String>("peerhbase2", TEST_UTIL2.getClusterKey());
    this.admin.addCluster(HBASE2, TEST_UTIL1.getClusterKey());
    this.admin.addPeer(HBASE2, peer);
    String tableName = "testDeletePeers";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1").setScope(1));
    this.admin.createTable(desc);
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL1, true);
    // Just verify that it is not created in the base cluster
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL, false);
    // Should be available in test util_2 also
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL2, true);

    this.admin.deletePeers(HBASE2);
    ClusterInfo[] infos = this.admin.listClusters();
    boolean tested = false;
    for (ClusterInfo info : infos) {
      if (info.getName().equals("hbase2")) {
        Assert.assertTrue(info.getPeers().size() == 0);
        tested = true;
      }
    }
    Assert.assertTrue(tested);
  }
}

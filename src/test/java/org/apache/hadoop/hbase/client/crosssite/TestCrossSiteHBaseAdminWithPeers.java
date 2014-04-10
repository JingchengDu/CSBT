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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteHBaseAdminWithPeers {

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();
  private CrossSiteHBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL1.startMiniCluster(1);
    TEST_UTIL1.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL2.startMiniCluster(1);
    TEST_UTIL2.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL2.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
  }

  @Test
  public void testAddAndDeletePeers() throws Exception {
    String HBASE1 = "hbase1";
    this.admin.addCluster(HBASE1, TEST_UTIL1.getClusterKey());
    Pair<String, String> peer = new Pair<String, String>("peerhbase1", TEST_UTIL2.getClusterKey());
    this.admin.addPeer(HBASE1, peer);
    String tableName = "testAddPeers";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1").setScope(1));
    this.admin.createTable(desc);
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL1, true);
    // Just verify that it is not created in the base cluster
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL, false);
    // Should be available in test util_2 also
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL2, true);
    
    this.admin.disableTable(tableName);
    Assert.assertTrue(TEST_UTIL1.getHBaseAdmin().isTableDisabled(tableName+"_hbase1"));
    // enable the table and see if the table in the peer is also enabled
    this.admin.enableTable(tableName);
    Assert.assertTrue(TEST_UTIL1.getHBaseAdmin().isTableEnabled(tableName+"_hbase1"));
    this.admin.deletePeers("hbase1");
    // Should not be available in test util_2 also
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL2, false);
  }

}

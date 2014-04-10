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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHBaseAdmin;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(LargeTests.class)
public class TestCSBTVerifier {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private Configuration conf = TEST_UTIL1.getConfiguration();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL3 = new HBaseTestingUtility();
  private static ExecutorService exec = new ScheduledThreadPoolExecutor(10);
  private static String HBASE1 = "hbase1";
  private static String HBASE2 = "hbase2";
  private static String HBASE3 = "hbase3";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL1.startMiniCluster(1);
    TEST_UTIL1.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL2.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL2.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL2.startMiniCluster(1);
    TEST_UTIL2.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL2.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL3.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL3.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL3.startMiniCluster(1);
    TEST_UTIL3.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL2.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL1.getConfiguration());
    admin.addCluster(HBASE1, TEST_UTIL1.getClusterKey());
    admin.addCluster(HBASE2, TEST_UTIL2.getClusterKey());
    admin.addCluster(HBASE3, TEST_UTIL3.getClusterKey());

    admin.addPeer(HBASE2, new Pair<String, String>(HBASE1, TEST_UTIL1.getClusterKey()));
    admin.addPeer(HBASE3, new Pair<String, String>(HBASE1, TEST_UTIL1.getClusterKey()));
    admin.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL3.shutdownMiniCluster();
  }

  @Test
  public void testCreateMissingTableUsingClusterVerifier() throws Exception {
    CrossSiteHBaseAdmin csAdmin = new CrossSiteHBaseAdmin(TEST_UTIL1.getConfiguration());
    String tableName = "testCreateMissingTableUsingClusterVerifier";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    createTable(csAdmin, tableName, desc);

    desc = new HTableDescriptor(tableName + "_table2");
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    hcd.setScope(1);
    desc.addFamily(hcd);
    createTable(csAdmin, tableName + "_table2", desc);
    HBaseAdmin admin1 = new HBaseAdmin(TEST_UTIL1.getConfiguration());
    assertTrue(admin1.tableExists(tableName + "_table2_" + HBASE2));
    csAdmin.close();

    // Now delete a table directly in the cluster 2
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL2.getConfiguration());
    admin.disableTable(tableName + "_table2_" + HBASE2);
    admin.deleteTable(tableName + "_table2_" + HBASE2);
    assertFalse(admin.tableExists(tableName + "_table2_" + HBASE2));

    admin1.disableTable(tableName + "_table2_" + HBASE2);
    admin1.deleteTable(tableName + "_table2_" + HBASE2);
    assertFalse(admin1.tableExists(tableName + "_table2_" + HBASE2));

    CSBTClusterVerifier verifier = new CSBTClusterVerifier(conf, exec);
    verifier.connect();
    verifier.fixTables(true);
    verifier.verifyClusterAndTables();

    assertTrue(admin.tableExists(tableName + "_table2_" + HBASE2));
    assertTrue(admin1.tableExists(tableName + "_table2_" + HBASE2));
    admin.close();
    admin1.close();
  }
  
  @Test
  public void testModifyTheHTDForMismatchWithTheHTDZNodeUsingClusterVerifier() throws Exception {
    CrossSiteHBaseAdmin csAdmin = new CrossSiteHBaseAdmin(TEST_UTIL1.getConfiguration());
    String tableName = "testModifyTheHTDForMismatchWithTheHTDZNodeUsingClusterVerifiers";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    createTable(csAdmin, tableName, desc);

    desc = new HTableDescriptor(tableName + "_table2");
    desc.addFamily(new HColumnDescriptor("col1"));
    createTable(csAdmin, tableName + "_table2", desc);
    csAdmin.close();
   
    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("col1");
    //Change the scope
    hColumnDescriptor.setScope(1);
    
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL2.getConfiguration());
    admin.disableTable(tableName + "_table2_" + HBASE2);
    admin.modifyColumn(tableName + "_table2_" + HBASE2, hColumnDescriptor);
    admin.enableTable(tableName + "_table2_" + HBASE2);
    
    CSBTClusterVerifier verifier = new CSBTClusterVerifier(conf, exec);
    verifier.connect();
    verifier.fixHTDs(true);
    verifier.verifyHTDs();

    HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName
        + "_table2_" + HBASE2));
    HColumnDescriptor family = tableDescriptor.getFamily(Bytes.toBytes("col1"));
    assertTrue(family.getScope() == 0);
    admin.close();
  }
  
  @Test
  public void testTableStatusInMainAndPeerCluster() throws Exception {
    CrossSiteHBaseAdmin csAdmin = new CrossSiteHBaseAdmin(TEST_UTIL1.getConfiguration());
    String tableName = "testTableStatusInMainAndPeerCluster";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    hcd.setScope(1);
    desc.addFamily(hcd);
    createTable(csAdmin, tableName, desc);
    csAdmin.close();
    
    HBaseAdmin admin1 = new HBaseAdmin(TEST_UTIL1.getConfiguration());
    String nameAsString = "testTableStatusInMainAndPeerCluster_hbase3";
    admin1.disableTable(nameAsString);
    HBaseAdmin admin2 = new HBaseAdmin(TEST_UTIL2.getConfiguration());
    String nameAsString2 = "testTableStatusInMainAndPeerCluster_hbase2";
    admin2.disableTable(nameAsString2);

    CSBTClusterVerifier verifier = new CSBTClusterVerifier(conf, exec);
    verifier.connect();
    verifier.fixTableStates(true);
    verifier.verifyTableStatesInClusterAndPeers();

    assertTrue("The table in the peer should be enabled again", admin1.isTableEnabled(nameAsString));
    assertTrue("The table in the peer should be enabled again",
        admin2.isTableEnabled(nameAsString2));
    admin1.close();
    admin2.close();
  }

  protected void createTable(CrossSiteHBaseAdmin admin, String tableName, HTableDescriptor desc)
      throws IOException {
    admin.createTable(desc);
    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
  }
}

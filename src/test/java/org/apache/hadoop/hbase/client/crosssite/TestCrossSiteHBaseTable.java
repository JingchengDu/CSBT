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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteHBaseTable {
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
    this.admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
  }

  @Test
  public void testPutAndScan() throws Exception {
    String HBASE1 = "hbase1";
    this.admin.addCluster(HBASE1, TEST_UTIL1.getClusterKey());
    Pair<String, String> peer = new Pair<String, String>("peerhbase1", TEST_UTIL2.getClusterKey());
    this.admin.addPeer(HBASE1, peer);
    String tableName = "testPutAndScan";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1").setScope(1));
    this.admin.createTable(desc);
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL1, true);
    // Just verify that it is not created in the base cluster
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL, false);
    // Should be available in test util_2 also
    TestCrossSiteHBaseAdmin.waitUntilAllRegionsAssigned(Bytes.toBytes(tableName), TEST_UTIL2, true);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(this.admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("100"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase1,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q2"), Bytes.toBytes("100"));
    crossSiteHTable.put(p);

    Get get = new Get(Bytes.toBytes("hbase1,india"));
    Result result = crossSiteHTable.get(get);
    byte[] value = result.getValue(Bytes.toBytes("col1"), Bytes.toBytes("q2"));
    Assert.assertTrue(Bytes.equals(value, Bytes.toBytes("100")));
    
    TEST_UTIL1.shutdownMiniCluster();
    //Thread.sleep(3000);
    result = crossSiteHTable.get(get);
    value = result.getValue(Bytes.toBytes("col1"), Bytes.toBytes("q2"));
    Assert.assertTrue(Bytes.equals(value, Bytes.toBytes("100")));
    
    Scan s = new Scan();
    s.setCaching(1);
    ResultScanner scanner = crossSiteHTable.getScanner(s);
    Result next = scanner.next();
    Assert.assertTrue(next != null);
    TEST_UTIL1.shutdownMiniCluster();
    Thread.sleep(6*1000);
    next = scanner.next();
    Assert.assertTrue(next != null);
    next = scanner.next();
    Assert.assertNull(next);
    HTable table = new HTable(TEST_UTIL2.getConfiguration(), Bytes.toBytes(tableName + "_hbase1"));
    try {
      while (true) {
        s = new Scan();
        scanner = table.getScanner(s);
        Result[] results = scanner.next(2);
        if ((results != null && results.length == 2)) {
          break;
        }
        Thread.sleep(500);
      }
    } finally {
      table.close();
    }
    TEST_UTIL1.shutdownMiniCluster();
    Thread.sleep(6*1000);
    // Still the read should be served from the Peer
    s = new Scan();
    scanner = crossSiteHTable.getScanner(s);
    next = scanner.next();
    Assert.assertTrue(next != null);
    next = scanner.next();
    Assert.assertTrue(next != null);
    next = scanner.next();
    Assert.assertNull(next);
    crossSiteHTable.close();
  }
}

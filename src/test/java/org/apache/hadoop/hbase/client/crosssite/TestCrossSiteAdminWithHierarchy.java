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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteAdminWithHierarchy {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL3 = new HBaseTestingUtility();
  private CrossSiteHBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL1.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL1.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
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
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL3.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL3.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL3.startMiniCluster(1);
    TEST_UTIL3.getConfiguration().setStrings(
        "hbase.crosssite.global.zookeeper",
        "localhost:" + TEST_UTIL1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");
  }

  @Before
  public void setUpBefore() throws Exception {
    admin = new CrossSiteHBaseAdmin(TEST_UTIL1.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL3.shutdownMiniCluster();
  }

  @Test
  public void testAddHierarchy() throws Exception {
    String tableName = "testAddHierarchy";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
    String HBASE1 = "hbase1";
    String HBASE2 = "hbase2";
    try {
      admin.addCluster(HBASE1, TEST_UTIL2.getClusterKey());
      admin.addCluster(HBASE2, TEST_UTIL3.getClusterKey());

      // Add hierarchy to it
      String[] hierarchy = new String[2];
      hierarchy[0] = "hbase2";
      admin.addHierarchy("hbase1", hierarchy);
      // Do some puts and do a scan
      // Check whether the scan considers the hierarchy also
      // A new table has to be created here, other wise the hierarchy inofmration will not be upated
      crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
      Put p = new Put(Bytes.toBytes("hbase1,china"));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("100"));
      crossSiteHTable.put(p);

      p = new Put(Bytes.toBytes("hbase1,india"));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("q2"), Bytes.toBytes("100"));
      crossSiteHTable.put(p);

      Scan s = new Scan();
      String[] clusterNames = new String[1];
      clusterNames[0] = HBASE1;
      ResultScanner scanner = crossSiteHTable.getScanner(s, clusterNames);
      Result next = scanner.next();
      Assert.assertTrue(next != null);
      crossSiteHTable.close();
    } finally {
      admin.deleteCluster(HBASE1);
    }
  }
}

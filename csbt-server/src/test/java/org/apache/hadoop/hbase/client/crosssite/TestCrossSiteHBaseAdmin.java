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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteHBaseAdmin {

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
  public void testCreateTablesOnly() throws Exception {
    String tableName = "testCreateTables";
    HTableDescriptor desc = new HTableDescriptor("testCreateTables");

    desc.addFamily(new HColumnDescriptor("col1"));

    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
  }

  @Test
  public void testAddClusterWithExistingTables() throws Exception {
    String tableName = "testAddClusterWithExistingTables";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
    String HBASE1 = "hbase1";
    try {
      admin.addCluster(HBASE1, TEST_UTIL2.getClusterKey());

      // Test disable and enable cluster in this
      admin.disableTable(tableName);
      admin.enableTable(tableName);
    } finally {
      admin.deleteCluster(HBASE1);
    }
  }

  @Test
  public void testCreateTableOnExistingCluster() throws Exception {
    String tableName = "testCreateTableOnExistingCluster";
    String HBASE2 = "hbase2";
    try {
      admin.addCluster(HBASE2, TEST_UTIL2.getClusterKey());

      HTableDescriptor desc = new HTableDescriptor("testCreateTableOnExistingCluster");
      desc.addFamily(new HColumnDescriptor("col1"));
      admin.createTable(desc);

      CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
      Assert.assertNotNull(crossSiteHTable);
    } finally {
      admin.deleteCluster(HBASE2);
    }
  }

  @Test
  public void testDuplicateAddClusterShouldThrowException() throws Exception {
    String HBASE3 = "hbase3";
    admin.addCluster(HBASE3, TEST_UTIL2.getClusterKey());
    try {
      admin.addCluster(HBASE3, TEST_UTIL2.getClusterKey());
      fail("Should fail if dup cluster is added");
    } catch (Exception e) {
    }
    try {
      admin.addCluster("dup", TEST_UTIL2.getClusterKey());
      fail("Should fail if dup cluster is added");
    } catch (Exception e) {
    }
    admin.deleteCluster(HBASE3);
  }

  @Test
  public void testCreateTableOnExistingClusterWithSplitKeys() throws Exception {
    String tableName = "testCreateTableOnExistingClusterWithSplitKeys";
    String HBASE3 = "hbase3";
    String HBASE4 = "hbase4";
    admin.addCluster(HBASE3, TEST_UTIL2.getClusterKey());
    admin.addCluster(HBASE4, TEST_UTIL3.getClusterKey());
    byte [][] splitKeys = {
        new byte [] { 1, 1, 1 },
        new byte [] { 2, 2, 2 },
        new byte [] { 3, 3, 3 },
        new byte [] { 4, 4, 4 },
        new byte [] { 5, 5, 5 },
        new byte [] { 6, 6, 6 },
        new byte [] { 7, 7, 7 },
        new byte [] { 8, 8, 8 },
        new byte [] { 9, 9, 9 },
    };

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc, splitKeys);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Assert.assertNotNull(crossSiteHTable);
    ensureThatTheSplitKeysAreDiff(TEST_UTIL2.getConfiguration(), TEST_UTIL3.getConfiguration(),
        tableName);
    admin.deleteCluster(HBASE3);
    admin.deleteCluster(HBASE4);
  }
  
  private void ensureThatTheSplitKeysAreDiff(Configuration conf1, Configuration conf2,
      String tableName) throws IOException {
    List<HRegionInfo> table1Meta = scanMetaWithGivenUtil(conf1, tableName);
    List<HRegionInfo> table2Meta = scanMetaWithGivenUtil(conf2, tableName);
    boolean shouldBeDifferent  = true;
    for(int i = 0; i < table1Meta.size() ; i++) {
      if(table1Meta.get(i).equals(table2Meta.get(i))) {
        shouldBeDifferent = false;
      }
    }
    Assert.assertTrue(shouldBeDifferent);
  }

  protected List<HRegionInfo> scanMetaWithGivenUtil(Configuration conf, String tableName)
      throws IOException {
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    try {
      Scan scan = new Scan();
      scan.addFamily(HConstants.CATALOG_FAMILY);
      ResultScanner s = meta.getScanner(scan);

      List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
      try {
        Result r;
        while ((r = s.next()) != null) {
          Assert.assertTrue(r != null);
          byte[] b = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = MetaScanner.getHRegionInfo(r);
          System.out.println(info.getTable().getNameAsString());
          if (info.getTable().getNameAsString().startsWith(tableName)) {
            regionInfos.add(info);
          }
        }
        return regionInfos;
      } finally {
        s.close();
      }

    } finally {
      meta.close();
    }
  }
}

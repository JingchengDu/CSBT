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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestCrossSiteHBaseTable {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getConfiguration().set(
            CrossSiteConstants.CROSS_SITE_ZOOKEEPER,
            "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
                + ":/hbase");

    TEST_UTIL1.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL1.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL1.startMiniCluster(1);
    TEST_UTIL1.getConfiguration().set(
            CrossSiteConstants.CROSS_SITE_ZOOKEEPER,
            "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
                + ":/hbase");

    TEST_UTIL2.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL2.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL2.startMiniCluster(1);
    TEST_UTIL2.getConfiguration().set(
            CrossSiteConstants.CROSS_SITE_ZOOKEEPER,
            "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
                + ":/hbase");
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String hbase1 = "hbase1";
    String hbase2 = "hbase2";
    admin.addCluster(hbase1, TEST_UTIL1.getClusterKey());
    admin.addCluster(hbase2, TEST_UTIL2.getClusterKey());
    admin.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPutAndScan() throws Exception {
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "testPutAndScan";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);
    admin.close();

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("100"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase1,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q2"), Bytes.toBytes("101"));
    crossSiteHTable.put(p);

    Get get = new Get(Bytes.toBytes("hbase1,india"));
    Result result = crossSiteHTable.get(get);
    byte[] value = result.getValue(Bytes.toBytes("col1"), Bytes.toBytes("q2"));
    Assert.assertTrue(Bytes.equals(value, Bytes.toBytes("101")));

    Scan s = new Scan();
    s.setCaching(1);
    ResultScanner scanner = crossSiteHTable.getScanner(s);
    Result next = scanner.next();
    Assert.assertTrue(next != null);
    next = scanner.next();
    Assert.assertTrue(next != null);
    next = scanner.next();
    Assert.assertNull(next);
    crossSiteHTable.close();
  }

  @Test
  public void testIncrementColumnValue() throws Exception {
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "testIncrementColumnValue";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes(100l));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase2,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes(100l));
    crossSiteHTable.put(p);

    crossSiteHTable.incrementColumnValue(Bytes.toBytes("hbase1,china"), Bytes.toBytes("col1"),
        Bytes.toBytes("q1"), 1, Durability.USE_DEFAULT);
    crossSiteHTable.close();

    HTable table1 = new HTable(TEST_UTIL1.getConfiguration(), Bytes.toBytes(tableName + "_hbase1"));
    Get get1 = new Get(Bytes.toBytes("hbase1,china"));
    Result r1 = table1.get(get1);
    Assert.assertEquals(101, Bytes.toLong(r1.getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    HTable table2 = new HTable(TEST_UTIL2.getConfiguration(), Bytes.toBytes(tableName + "_hbase2"));
    Get get2 = new Get(Bytes.toBytes("hbase2,india"));
    Result r2 = table2.get(get2);
    Assert.assertEquals(100, Bytes.toLong(r2.getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    table1.close();
    table2.close();
    admin.close();
  }

  @Test
  public void testBatch() throws Exception {
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "testBatch";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("china"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase1,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("india"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase2,us"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("us"));
    crossSiteHTable.put(p);

    List<Row> actions = new ArrayList<Row>();
    Get get = new Get(Bytes.toBytes("hbase1,china"));
    get.addFamily(Bytes.toBytes("col1"));
    actions.add(get);
    actions.add(new Get(Bytes.toBytes("hbase1,india")));
    actions.add(new Get(Bytes.toBytes("hbase2,us")));
    Object[] results = new Object[3];
    crossSiteHTable.batch(actions, results);
    Assert.assertEquals("china",
        Bytes.toString(((Result) results[0]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    Assert.assertEquals("india",
        Bytes.toString(((Result) results[1]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    Assert.assertEquals("us",
        Bytes.toString(((Result) results[2]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    crossSiteHTable.close();
    admin.close();
  }

  @Test
  public void testBatchWithErrors() throws Exception {
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "testBatchWithErrors";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("china"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase1,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("india"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase2,us"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("us"));
    crossSiteHTable.put(p);

    List<Row> actions = new ArrayList<Row>();
    Get get = new Get(Bytes.toBytes("hbase1,china"));
    get.addFamily(Bytes.toBytes("col2"));
    actions.add(get);
    actions.add(new Get(Bytes.toBytes("hbase1,india")));
    actions.add(new Get(Bytes.toBytes("hbase2,us")));
    Object[] results = new Object[3];
    boolean hasErrors = false;
    try {
      crossSiteHTable.batch(actions, results);
    } catch (Exception e) {
      hasErrors = true;
    }
    Assert.assertTrue(hasErrors);
    Assert.assertTrue(results[0] instanceof Exception);
    Assert.assertEquals("india",
        Bytes.toString(((Result) results[1]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    Assert.assertEquals("us",
        Bytes.toString(((Result) results[2]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    crossSiteHTable.close();
    admin.close();
  }

  @Test
  public void testBatchWithErrosAndCallback() throws Exception {
    CrossSiteHBaseAdmin admin = new CrossSiteHBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "testBatchWithErrosAndCallback";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("col1"));
    admin.createTable(desc);

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("hbase1,china"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("china"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase1,india"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("india"));
    crossSiteHTable.put(p);

    p = new Put(Bytes.toBytes("hbase2,us"));
    p.add(Bytes.toBytes("col1"), Bytes.toBytes("q1"), Bytes.toBytes("us"));
    crossSiteHTable.put(p);

    List<Row> actions = new ArrayList<Row>();
    Get get = new Get(Bytes.toBytes("hbase1,china"));
    get.addFamily(Bytes.toBytes("col2"));
    actions.add(get);
    actions.add(new Get(Bytes.toBytes("hbase1,india")));
    actions.add(new Get(Bytes.toBytes("hbase2,us")));
    Object[] results = new Object[3];
    boolean hasErrors = false;
    TestBatchCallback callback = new TestBatchCallback();
    try {
      crossSiteHTable.batchCallback(actions, results, callback);
    } catch (Exception e) {
      hasErrors = true;
    }
    Assert.assertTrue(hasErrors);
    Assert.assertEquals(2, callback.getCount());
    Assert.assertTrue(results[0] instanceof Exception);
    Assert.assertEquals("india",
        Bytes.toString(((Result) results[1]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    Assert.assertEquals("us",
        Bytes.toString(((Result) results[2]).getValue(Bytes.toBytes("col1"), Bytes.toBytes("q1"))));
    crossSiteHTable.close();
    admin.close();
  }

  public static class TestBatchCallback implements Callback<Result> {

    private AtomicInteger counter = new AtomicInteger();

    public int getCount() {
      return counter.get();
    }

    @Override
    public void update(byte[] region, byte[] row, Result result) {
      counter.incrementAndGet();
    }

  }
}

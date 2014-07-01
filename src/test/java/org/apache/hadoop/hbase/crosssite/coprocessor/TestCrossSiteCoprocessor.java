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
package org.apache.hadoop.hbase.crosssite.coprocessor;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHBaseAdmin;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHTable;
import org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpointWithErrors;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ProtobufCoprocessorService;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos.SumResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithErrorsProtos;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.ServiceException;

public class TestCrossSiteCoprocessor {

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL1 = new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL2 = new HBaseTestingUtility();

  private static final String TABLE_NAME = "testCrossSiteCoprocessor";
  private static final byte[] CF = Bytes.toBytes("col1");
  private static final byte[] QN = Bytes.toBytes("q1");
  private static byte[][] SPLIT_KEYS = makeN(Bytes.toBytes(""), 10);
  private static byte[][] ROWS_HBASE1 = makeN(Bytes.toBytes("hbase1,"), 10);
  private static byte[][] ROWS_HBASE2 = makeN(Bytes.toBytes("hbase2,"), 10);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName(),
        ColumnAggregationEndpointWithErrors.class.getName());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getConfiguration().set(
        CrossSiteConstants.CROSS_SITE_ZOOKEEPER,
        "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL1.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL1.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL1.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName(),
        ColumnAggregationEndpointWithErrors.class.getName());
    TEST_UTIL1.getConfiguration().setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
    TEST_UTIL1.startMiniCluster(1);
    TEST_UTIL1.getConfiguration().set(
        CrossSiteConstants.CROSS_SITE_ZOOKEEPER,
        "localhost:" + TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT)
            + ":/hbase");

    TEST_UTIL2.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL2.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL2.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName(),
        ColumnAggregationEndpointWithErrors.class.getName());
    TEST_UTIL2.getConfiguration().setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
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

    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
    desc.addFamily(new HColumnDescriptor(CF));
    admin.createTable(desc, SPLIT_KEYS);
    admin.close();

    CrossSiteHTable crossSiteHTable = new CrossSiteHTable(admin.getConfiguration(), TABLE_NAME);
    for (int i = 0; i < 10; i++) {
      Put put = new Put(ROWS_HBASE1[i]);
      put.add(CF, QN, Bytes.toBytes(i + 1));
      crossSiteHTable.put(put);
    }
    for (int i = 0; i < 10; i++) {
      Put put = new Put(ROWS_HBASE2[i]);
      put.add(CF, QN, Bytes.toBytes(i + 1));
      crossSiteHTable.put(put);
    }
    crossSiteHTable.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL2.shutdownMiniCluster();
    TEST_UTIL1.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBatchCoprocessorWithoutStartStopKey() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], SumResponse> results = sumInBatch(table, null, CF, QN, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW);
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 1; i < 11; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testBatchCoprocessorWithPartKeys() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], SumResponse> results = sumInBatch(table, null, CF, QN, Bytes.toBytes("hbase1,00"),
        Bytes.toBytes("hbase1,05"));
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 1; i <= 6; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testBatchCoprocessorWithClusterAndPartKeys() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], SumResponse> results = sumInBatch(table, new String[] { "hbase1", "hbase2" }, CF,
        QN, Bytes.toBytes("00"), Bytes.toBytes("05"));
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 1; i <= 6; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testBatchCoprocessorWithErrors() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    ColumnAggregationWithErrorsProtos.SumRequest.Builder builder = ColumnAggregationWithErrorsProtos.SumRequest
        .newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(CF));
    if (QN != null && QN.length > 0) {
      builder.setQualifier(HBaseZeroCopyByteString.wrap(QN));
    }
    final Map<byte[], ColumnAggregationWithErrorsProtos.SumResponse> results = Collections
        .synchronizedMap(new TreeMap<byte[], ColumnAggregationWithErrorsProtos.SumResponse>(
            Bytes.BYTES_COMPARATOR));
    boolean hasErrors = false;
    try {
      table.batchCoprocessorService(
          ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors.getDescriptor()
              .findMethodByName("sum"), builder.build(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW, null, ColumnAggregationWithErrorsProtos.SumResponse
              .getDefaultInstance(), new Callback<ColumnAggregationWithErrorsProtos.SumResponse>() {

            @Override
            public void update(byte[] region, byte[] row,
                ColumnAggregationWithErrorsProtos.SumResponse result) {
              if (region != null) {
                results.put(region, result);
              }
            }
          });
    } catch (Exception e) {
      hasErrors = true;
    }
    assertEquals(true, hasErrors);
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], ColumnAggregationWithErrorsProtos.SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 1; i < 10; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testCoprocessorWithoutStartStopKey() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], Long> results = sum(table, null, CF, QN, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW);
    long sumResult = 0;
    long expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().longValue() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().longValue();
    }
    for (int i = 1; i < 11; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testCoprocessorWithPartKeys() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], Long> results = sum(table, null, CF, QN, Bytes.toBytes("hbase1,00"),
        Bytes.toBytes("hbase1,05"));
    long sumResult = 0;
    long expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().longValue() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().longValue();
    }
    for (int i = 1; i <= 6; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testCoprocessorWithClusterAndPartKeys() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    Map<byte[], Long> results = sum(table, new String[] { "hbase1", "hbase2" }, CF, QN,
        Bytes.toBytes("00"), Bytes.toBytes("05"));
    long sumResult = 0;
    long expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().longValue() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().longValue();
    }
    for (int i = 1; i <= 6; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testCoprocessorWithErrors() throws Throwable {
    CrossSiteHTable table = new CrossSiteHTable(TEST_UTIL.getConfiguration(), TABLE_NAME);
    ColumnAggregationWithErrorsProtos.SumRequest.Builder builder = ColumnAggregationWithErrorsProtos.SumRequest
        .newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(CF));
    if (QN != null && QN.length > 0) {
      builder.setQualifier(HBaseZeroCopyByteString.wrap(QN));
    }
    final Map<byte[], Long> results = Collections.synchronizedMap(new TreeMap<byte[], Long>(
        Bytes.BYTES_COMPARATOR));
    boolean hasErrors = false;
    try {
      table
          .coprocessorService(
              ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors.class,
              HConstants.EMPTY_START_ROW,
              HConstants.EMPTY_END_ROW,
              new Batch.Call<ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors, Long>() {
                @Override
                public Long call(
                    ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors instance)
                    throws IOException {
                  BlockingRpcCallback<ColumnAggregationWithErrorsProtos.SumResponse> rpcCallback = new BlockingRpcCallback<ColumnAggregationWithErrorsProtos.SumResponse>();
                  ColumnAggregationWithErrorsProtos.SumRequest.Builder builder = ColumnAggregationWithErrorsProtos.SumRequest
                      .newBuilder();
                  builder.setFamily(HBaseZeroCopyByteString.wrap(CF));
                  if (QN != null && QN.length > 0) {
                    builder.setQualifier(HBaseZeroCopyByteString.wrap(QN));
                  }
                  instance.sum(null, builder.build(), rpcCallback);
                  return rpcCallback.get().getSum();
                }
              }, new Batch.Callback<Long>() {
                public void update(byte[] region, byte[] row, Long value) {
                  results.put(region, value);
                }
              });
    } catch (Exception e) {
      hasErrors = true;
    }
    assertEquals(true, hasErrors);
    long sumResult = 0;
    long expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().longValue() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().longValue();
    }
    for (int i = 1; i < 10; i++) {
      expectedResult += i;
    }
    expectedResult *= 2;
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }

  private Map<byte[], SumResponse> sumInBatch(final CrossSiteHTable table,
      final String[] clusterNames, final byte[] family, final byte[] qualifier, final byte[] start,
      final byte[] end) throws ServiceException, Throwable {
    ColumnAggregationProtos.SumRequest.Builder builder = ColumnAggregationProtos.SumRequest
        .newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(family));
    if (qualifier != null && qualifier.length > 0) {
      builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
    }
    final Map<byte[], ColumnAggregationProtos.SumResponse> results = Collections
        .synchronizedMap(new TreeMap<byte[], ColumnAggregationProtos.SumResponse>(
            Bytes.BYTES_COMPARATOR));
    table.batchCoprocessorService(ColumnAggregationProtos.ColumnAggregationService.getDescriptor()
        .findMethodByName("sum"), builder.build(), start, end, clusterNames,
        ColumnAggregationProtos.SumResponse.getDefaultInstance(),
        new Callback<ColumnAggregationProtos.SumResponse>() {

          @Override
          public void update(byte[] region, byte[] row, ColumnAggregationProtos.SumResponse result) {
            if (region != null) {
              results.put(region, result);
            }
          }
        });
    return results;
  }

  private Map<byte[], Long> sum(final CrossSiteHTable table, final String[] clusterNames,
      final byte[] family, final byte[] qualifier, final byte[] start, final byte[] end)
      throws ServiceException, Throwable {
    ColumnAggregationProtos.SumRequest.Builder builder = ColumnAggregationProtos.SumRequest
        .newBuilder();
    builder.setFamily(HBaseZeroCopyByteString.wrap(family));
    if (qualifier != null && qualifier.length > 0) {
      builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
    }
    final Map<byte[], Long> results = Collections.synchronizedMap(new TreeMap<byte[], Long>(
        Bytes.BYTES_COMPARATOR));
    table.coprocessorService(ColumnAggregationProtos.ColumnAggregationService.class, start, end,
        clusterNames, new Batch.Call<ColumnAggregationProtos.ColumnAggregationService, Long>() {
          @Override
          public Long call(ColumnAggregationProtos.ColumnAggregationService instance)
              throws IOException {
            BlockingRpcCallback<ColumnAggregationProtos.SumResponse> rpcCallback = new BlockingRpcCallback<ColumnAggregationProtos.SumResponse>();
            ColumnAggregationProtos.SumRequest.Builder builder = ColumnAggregationProtos.SumRequest
                .newBuilder();
            builder.setFamily(HBaseZeroCopyByteString.wrap(family));
            if (qualifier != null && qualifier.length > 0) {
              builder.setQualifier(HBaseZeroCopyByteString.wrap(qualifier));
            }
            instance.sum(null, builder.build(), rpcCallback);
            return rpcCallback.get().getSum();
          }
        }, new Batch.Callback<Long>() {
          public void update(byte[] region, byte[] row, Long value) {
            results.put(region, value);
          }
        });
    return results;
  }
}

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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.crosssite.CrossSiteConstants;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * A simple pool of CrossSiteHTable instances.
 * 
 * Each CrossSiteHTablePool acts as a pool for all cross site tables. To use, instantiate an
 * CrossSiteHTablePool and use {@link #getTable(String)} to get a CrossSiteHTable from the pool.
 *
 * Once you are done with it, close your instance of {@link HTableInterface}
 * by calling {@link HTableInterface#close()} rather than returning the tables
 * to the pool with (deprecated) {@link #putTable(HTableInterface)}.
 * 
 * <p>
 * A pool can be created with a <i>maxSize</i> which defines the most HTable
 * references that will ever be retained for each table. Otherwise the default
 * is {@link Integer#MAX_VALUE}.
 * 
 * <p>
 * Pool will manage its own connections to the cluster. See
 * {@link HConnectionManager}.
 */
public class CrossSiteHTablePool implements Closeable {
  private final PoolMap<String, CrossSiteHTableInterface> tables;
  private final int maxSize;
  private final PoolType poolType;
  private final Configuration conf;
  private final HTableInterfaceFactory tableFactory;

  /**
   * Constructor to set maximum versions and use the specified configuration.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   */
  public CrossSiteHTablePool(final Configuration config, final int maxSize) {
    this(config, maxSize, null, null);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration and
   * table factory.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param tableFactory
   *          table factory
   */
  public CrossSiteHTablePool(final Configuration config, final int maxSize,
      final HTableInterfaceFactory tableFactory) {
    this(config, maxSize, tableFactory, PoolType.Reusable);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration and
   * pool type.
   * 
   * @param config
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param poolType
   *          pool type which is one of {@link PoolType#Reusable} or
   *          {@link PoolType#ThreadLocal}
   */
  public CrossSiteHTablePool(final Configuration config, final int maxSize,
      final PoolType poolType) {
    this(config, maxSize, null, poolType);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration,
   * table factory and pool type. The HTablePool supports the
   * {@link PoolType#Reusable} and {@link PoolType#ThreadLocal}. If the pool
   * type is null or not one of those two values, then it will default to
   * {@link PoolType#Reusable}.
   * 
   * @param conf
   *          configuration
   * @param maxSize
   *          maximum number of references to keep for each table
   * @param tableFactory
   *          table factory
   * @param poolType
   *          pool type which is one of {@link PoolType#Reusable} or
   *          {@link PoolType#ThreadLocal}
   * @throws RuntimeException  
   */
  public CrossSiteHTablePool(final Configuration conf, final int maxSize,
      final HTableInterfaceFactory tableFactory, PoolType poolType) {
    try {
      this.conf = getCrossSiteConf(conf, conf.get(CrossSiteConstants.CROSS_SITE_ZOOKEEPER));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.maxSize = maxSize;
    if (tableFactory != null) {
      this.tableFactory = tableFactory;
    } else {
      Class<? extends HTableInterfaceFactory> hTableFactoryClass = (Class<? extends HTableInterfaceFactory>) conf
          .getClass(CrossSiteConstants.CROSS_SITE_HTABLE_FACTORY_CLASS, HTableFactory.class,
              HTableInterfaceFactory.class);
      try {
        Constructor<? extends HTableInterfaceFactory> constructor = hTableFactoryClass
            .getConstructor();
        this.tableFactory = constructor.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (poolType == null) {
      this.poolType = PoolType.Reusable;
    } else {
      switch (poolType) {
      case Reusable:
      case ThreadLocal:
        this.poolType = poolType;
        break;
      default:
        this.poolType = PoolType.Reusable;
        break;
      }
    }
    this.tables = new PoolMap<String, CrossSiteHTableInterface>(this.poolType, this.maxSize);
  }

  private Configuration getCrossSiteConf(Configuration conf, String clusterKey) throws IOException {
    // create the connection to the global zk
    Configuration crossSiteZKConf = new Configuration(conf);
    ZKUtil.applyClusterKeyToConf(crossSiteZKConf, clusterKey);
    return crossSiteZKConf;
  }

  /**
   * Get a reference to the specified cross site table from the pool.
   * 
   * @param tableName
   * @return a reference to the specified cross site table
   * @throws RuntimeException if there is a problem instantiating the CrossSiteHTable
   */
  public CrossSiteHTableInterface getTable(byte[] tableName) {
    CrossSiteHTableInterface table = findOrCreateTable(tableName);
    // return a proxy table so when user closes the proxy, the actual table
    // will be returned to the pool
    return new PooledHTable(table);
  }

  /**
   * Get a reference to the specified cross site table from the pool.
   * Create a new one if one is not available.
   * 
   * @param tableName
   * @return a reference to the specified table
   * @throws RuntimeException If there is a problem instantiating the HTable
   */
  private CrossSiteHTableInterface findOrCreateTable(byte[] tableName) {
    CrossSiteHTableInterface table = tables.get(tableName);
    if (table == null) {
      table = createHTable(tableName);
    }
    return table;
  }

  /**
   * Get a reference to the specified cross site table from the pool.
   * Create a new one if one is not available.
   * 
   * @param tableName
   * @return a reference to the specified cross site table
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public CrossSiteHTableInterface getTable(String tableName) {
    return getTable(Bytes.toBytes(tableName));
  }

  /**
   * Puts the specified HTable back into the pool.
   * <p>
   * 
   * If the pool already contains <i>maxSize</i> references to the table, then
   * the table instance gets closed after flushing buffered edits.
   * 
   * @param table
   *          table
   */
  private void returnTable(CrossSiteHTableInterface table) throws IOException {
    String tableName = Bytes.toString(table.getTableName());
    if (tables.size(tableName) >= maxSize) {
      // release table instance since we're not reusing it
      this.tables.remove(tableName, table);
      table.close();
      return;
    }
    tables.put(tableName, table);
  }

  protected CrossSiteHTableInterface createHTable(byte[] tableName) {
    try {
      return new CrossSiteHTable(this.conf, tableName, null, this.tableFactory);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  /**
   * Closes all the CrossSiteHTable instances , belonging to the given table, in the
   * table pool.
   * <p>
   * Note: this is a 'shutdown' of the given table pool and different from
   * {@link #putTable(CrossSiteHTableInterface)}, that is used to return the table
   * instance to the pool for future re-use.
   * 
   * @param tableName
   */
  public void closeTablePool(final String tableName) throws IOException {
    Collection<CrossSiteHTableInterface> tables = this.tables.values(tableName);
    if (tables != null) {
      for (CrossSiteHTableInterface table : tables) {
        table.close();
      }
    }
    this.tables.remove(tableName);
  }

  /**
   * See {@link #closeTablePool(String)}.
   * 
   * @param tableName
   */
  public void closeTablePool(final byte[] tableName) throws IOException {
    closeTablePool(Bytes.toString(tableName));
  }

  /**
   * Closes all the CrossSiteHTable instances , belonging to all tables in the table
   * pool.
   * <p>
   * Note: this is a 'shutdown' of all the table pools.
   */
  public void close() throws IOException {
    for (String tableName : tables.keySet()) {
      closeTablePool(tableName);
    }
    this.tables.clear();
  }

  public int getCurrentPoolSize(String tableName) {
    return tables.size(tableName);
  }

  /**
   * A proxy class that extends CrossSiteHTable. Call close method to return the
   * wrapped table back to the table pool
   */
  class PooledHTable implements CrossSiteHTableInterface {

    private CrossSiteHTableInterface table; // actual table implementation

    public PooledHTable(CrossSiteHTableInterface table) {
      this.table = table;
    }

    @Override
    public byte[] getTableName() {
      return table.getTableName();
    }

    @Override
    public TableName getName() {
      return table.getName();
    }

    @Override
    public Configuration getConfiguration() {
      return table.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
      return table.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
      return table.exists(get);
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
      return table.exists(gets);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException,
        InterruptedException {
      table.batch(actions, results);
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException,
        InterruptedException {
      return table.batch(actions);
    }

    @Override
    public <R> void batchCallback(
        final List<?extends Row> actions, final Object[] results, final Callback<R> callback)
        throws IOException, InterruptedException {
      table.batchCallback(actions, results, callback);
    }

    @Override
    public <R> Object[] batchCallback(
        List<? extends Row> actions, Callback<R> callback) throws IOException, InterruptedException {
      return table.batchCallback(actions, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
      return table.get(get);
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
      return table.get(gets);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
      return table.getRowOrBefore(row, family);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
      return table.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
      return table.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier)
        throws IOException {
      return table.getScanner(family, qualifier);
    }

    @Override
    public void put(Put put) throws IOException {
      table.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
      table.put(puts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
        byte[] value, Put put) throws IOException {
      return table.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
      table.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
      table.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
        byte[] value, Delete delete) throws IOException {
      return table.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
      return table.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
        byte[] qualifier, long amount) throws IOException {
      return table.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
        long amount, Durability durability) throws IOException {
      return table.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family,
        byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
      return table.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
      return table.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
      table.flushCommits();
    }

    /**
     * Returns the actual table back to the pool
     * 
     * @throws IOException
     */
    public void close() throws IOException {
      returnTable(table);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
        Class<T> protocol, byte[] startKey, byte[] endKey,
        Batch.Call<T, R> callable) throws IOException, Throwable {
      return table.coprocessorService(protocol, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(
        Class<T> protocol, byte[] startKey, byte[] endKey,
        Batch.Call<T, R> callable, Batch.Callback<R> callback)
        throws IOException, Throwable {
      table.coprocessorService(protocol, startKey, endKey, callable, callback);
    }

    @Override
    public String toString() {
      return "PooledHTable{" + ", table=" + table + '}';
    }

    /**
     * Expose the wrapped HTable to tests in the same package
     * 
     * @return wrapped htable
     */
    HTableInterface getWrappedTable() {
      return table;
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
      table.mutateRow(rm);
    }

    @Override
    public Result append(Append append) throws IOException {
      return table.append(append);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
      table.setAutoFlush(autoFlush);
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
      table.setAutoFlush(autoFlush, clearBufferOnFail);
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
      table.setAutoFlushTo(autoFlush);
    }

    @Override
    public long getWriteBufferSize() {
      return table.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
      table.setWriteBufferSize(writeBufferSize);
    }

    @Override
    public ResultScanner getScanner(Scan scan, String[] clusterNames) throws IOException {
      return table.getScanner(scan, clusterNames);
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
      return table.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> protocol,
        byte[] startKey, byte[] endKey, String[] clusterNames, Call<T, R> callable)
        throws IOException, Throwable {
      return table.coprocessorService(protocol, startKey, endKey, clusterNames, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> protocol,
        byte[] startKey, byte[] endKey, String[] clusterNames, Call<T, R> callable,
        Callback<R> callback) throws IOException, Throwable {
      table.coprocessorService(protocol, startKey, endKey, clusterNames, callable, callback);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
        Descriptors.MethodDescriptor methodDescriptor, Message request,
        byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
      return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
        Descriptors.MethodDescriptor methodDescriptor, Message request,
        byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
        throws ServiceException, Throwable {
      table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

  }
}

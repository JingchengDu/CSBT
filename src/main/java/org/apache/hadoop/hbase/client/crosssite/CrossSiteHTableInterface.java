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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Used to communicate with a single Cross site HBase table.
 */
public interface CrossSiteHTableInterface extends HTableInterface {

  /**
   * Gets the <code>CrossSiteClientScanner</code>. Users could indicate the cluster names which the
   * scanner will scan against. All the child clusters including themselves in the hierarchy are
   * counted.
   * 
   * @param scan
   * @param clusterNames
   * @return
   * @throws IOException
   */
  ResultScanner getScanner(Scan scan, String[] clusterNames) throws IOException;

  /**
   * 
   * Execute the coprocessor.
   * 
   * The cluster names will be considered as a filter condition together with the start key and stop
   * key.
   * 
   * @param protocol
   * @param startKey
   * @param endKey
   * @param clusterNames
   * @param callable
   * @return
   * @throws IOException
   * @throws Throwable
   */
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, String[] clusterNames, Call<T, R> callable)
      throws IOException, Throwable;

  /**
   * Execute the coprocessor.
   * 
   * The cluster names will be considered as a filter condition together with the start key and stop
   * key.
   * 
   * @param protocol
   * @param startKey
   * @param endKey
   * @param clusterNames
   * @param callable
   * @param callback
   * @throws IOException
   * @throws Throwable
   */
  public <T extends CoprocessorProtocol, R> void coprocessorExec(final Class<T> protocol,
      final byte[] startKey, final byte[] endKey, final String[] clusterNames,
      final Call<T, R> callable, final Callback<R> callback) throws IOException, Throwable;
}
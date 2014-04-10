/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hbase.crosssite.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.crosssite.CrossSiteHTable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * Convert Map/Reduce output and write it to an HBase Cross Site. The KEY is ignored while the
 * output value <u>must</u> be either a {@link Put} or a {@link Delete} instance.
 * 
 * @param <KEY>
 *          The type of the key. Ignored in this class.
 */
public class CrossSiteTableOutputFormat<KEY> extends TableOutputFormat<KEY> {
  private final Log LOG = LogFactory.getLog(CrossSiteTableOutputFormat.class);

  @Override
  public void setConf(Configuration otherConf) {
    this.conf = HBaseConfiguration.create(otherConf);
    String tableName = this.conf.get(OUTPUT_TABLE);
    if (tableName == null || tableName.length() <= 0) {
      throw new IllegalArgumentException("Must specify table name");
    }
    String address = this.conf.get(QUORUM_ADDRESS);
    int zkClientPort = conf.getInt(QUORUM_PORT, 0);
    String serverClass = this.conf.get(REGION_SERVER_CLASS);
    String serverImpl = this.conf.get(REGION_SERVER_IMPL);
    try {
      if (address != null) {
        ZKUtil.applyClusterKeyToConf(this.conf, address);
      }
      if (serverClass != null) {
        this.conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
        this.conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
      }
      if (zkClientPort != 0) {
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
      }
      this.table = new CrossSiteHTable(this.conf, Bytes.toBytes(tableName));
      this.table.setAutoFlush(false);
      LOG.info("Created table instance for " + tableName);
    } catch (IOException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }
}

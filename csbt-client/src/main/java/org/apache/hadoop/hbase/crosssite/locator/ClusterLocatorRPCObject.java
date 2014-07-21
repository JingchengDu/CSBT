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
package org.apache.hadoop.hbase.crosssite.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

public class ClusterLocatorRPCObject implements Writable {
  private ClusterLocator locator;

  public ClusterLocatorRPCObject() {
    super();
  }

  public ClusterLocatorRPCObject(ClusterLocator locator) {
    this.locator = locator;
  }

  public ClusterLocator getClusterLocator() {
    return this.locator;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.locator = (ClusterLocator) createForName(Bytes.toString(Bytes.readByteArray(in)));
    this.locator.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, Bytes.toBytes(locator.getClass().getName()));
    locator.write(out);
  }

  @SuppressWarnings("unchecked")
  private Writable createForName(String className) {
    try {
      Class<? extends Writable> clazz = (Class<? extends Writable>) Class.forName(className);
      return WritableFactories.newInstance(clazz, new Configuration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    }
  }

}

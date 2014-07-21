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
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/**
 * Cluster locator that extracts the cluster name as the Nth substring of the row key split by the
 * specified delimiter. The delimiter has a default value as ",".
 * Examples
 * if clustername is cluster1 and index is 4 then
 * "china,india,usa,uk,cluster1,australia"
 * Here cluster1 is at the fourth index and it will be returned as the clustername
 */
public class CompositeSubstringClusterLocator extends AbstractClusterLocator {

  private String delimiter = ",";
  private int index = 0;
  private Pattern pattern = null;

  public CompositeSubstringClusterLocator() {
    super();
  }

  public CompositeSubstringClusterLocator(final String delimiter, final int index) {
    if (delimiter != null) {
      this.delimiter = delimiter;
    }
    this.index = index;
  }

  @Override
  public String getClusterName(byte[] row) throws RowNotLocatableException {
    String rowStr = Bytes.toString(row);
    if (rowStr == null || index < 0)
      throw new RowNotLocatableException(row);

    if (pattern == null) {
      pattern = Pattern.compile(delimiter);
    }
    // TODO : Is this + 2 needed here? 
    String[] splits = pattern.split(rowStr, index + 2);
    if (splits.length <= index)
      throw new RowNotLocatableException(row);

    return splits[index];
  }

  public String getDelimiter() {
    return delimiter;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    delimiter = Text.readString(in);
    index = in.readInt();
    pattern = null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, delimiter);
    out.writeInt(index);
  }

  @Override
  public ClusterLocator createClusterLocatorFromArguments(ArrayList<byte[]> locatorArguments) {
    if (locatorArguments.size() != 2) {
      throw new IllegalArgumentException(
          "Incorrect Arguments passed to CompositeSubstringClusterLocator. "
              + "Expected: 2 but got: " + locatorArguments.size());
    }
    byte[] delimiter = CrossSiteUtil.convertByteArrayToString(locatorArguments.get(0));
    int index = CrossSiteUtil.convertByteArrayToInt(locatorArguments.get(1));
    return new CompositeSubstringClusterLocator(Bytes.toString(delimiter), index);
  }

  @Override
  public byte[][] getSplitKeys(String clusterName, byte[][] splitKeys) throws IOException {
    if (index > 0 || index < 0) {
      return splitKeys;
    } else {
      byte[][] newSplitKeys = null;
      if (splitKeys != null) {
        newSplitKeys = new byte[splitKeys.length][];
        int index = 0;
        for (byte[] splitKey : splitKeys) {
          StringBuilder sr = new StringBuilder(clusterName);
          sr.append(delimiter);
          sr.append(Bytes.toString(splitKey));
          newSplitKeys[index++] = Bytes.toBytes(sr.toString());
        }
      }
      return newSplitKeys;
    }
  }

  @Override
  public void validateArguments(String[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Incorrect Arguments passed to CompositeSubstringClusterLocator. "
              + "Expected: 2 but got: " + args.length + ". Expected a delimiter and an index");
    }
    this.delimiter = Bytes.toString(CrossSiteUtil.convertByteArrayToString(Bytes.toBytes(args[0])));
    this.index = CrossSiteUtil.convertByteArrayToInt(Bytes.toBytes(args[1]));
  }
}

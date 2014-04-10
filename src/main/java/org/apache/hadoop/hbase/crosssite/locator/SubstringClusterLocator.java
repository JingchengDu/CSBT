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

import org.apache.hadoop.hbase.crosssite.CrossSiteUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Cluster locator that extracts the cluster name as the substring value of the row key according to
 * the specified start and end position.
 * The substring begins at the specified beginIndex and extends to the character at index endIndex - 1
 */
public class SubstringClusterLocator extends AbstractClusterLocator {

  private int start = 0;
  private int end = 0;

  public SubstringClusterLocator() {
    super();
  }

  public SubstringClusterLocator(int start, int end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public String getClusterName(byte[] row) throws RowNotLocatableException {
    String rowStr = Bytes.toString(row);
    if (rowStr == null || start < 0 || start >= rowStr.length() || end <= start)
      throw new RowNotLocatableException(row);

    return rowStr.substring(start, end > rowStr.length() ? rowStr.length() : end);
  }

  public int getStartPosition() {
    return start;
  }

  public int getEndPosition() {
    return end;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.start = in.readInt();
    this.end = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.start);
    out.writeInt(this.end);
  }

  @Override
  public ClusterLocator createClusterLocatorFromArguments(ArrayList<byte[]> locatorArguments) {
    if (locatorArguments.size() != 2) {
      throw new IllegalArgumentException("Incorrect Arguments passed to SubstringClusterLocator. "
          + "Expected: 2 but got: " + locatorArguments.size());
    }
    int start = CrossSiteUtil.convertByteArrayToInt(locatorArguments.get(0));
    int end = CrossSiteUtil.convertByteArrayToInt(locatorArguments.get(1));
    return new SubstringClusterLocator(start, end);
  }

  @Override
  public byte[][] getSplitKeys(String clusterName, byte[][] splitKeys) throws IOException {
    if (start > 0 || start < 0) {
      return splitKeys;
    } else {
      byte[][] newSplitKeys = null;
      if (splitKeys != null) {
        newSplitKeys = new byte[splitKeys.length][];
        int index = 0;
        for (byte[] splitKey : splitKeys) {
          StringBuilder sr = new StringBuilder(clusterName);
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
      throw new IllegalArgumentException("Incorrect Arguments passed to SubstringClusterLocator. "
          + "Expected: 2 but got: " + args.length + ". Expected start and end indices to "
          + " for substring");
    }
    this.start = CrossSiteUtil.convertByteArrayToInt(Bytes.toBytes(args[0]));
    this.end = CrossSiteUtil.convertByteArrayToInt(Bytes.toBytes(args[1]));
  }
}

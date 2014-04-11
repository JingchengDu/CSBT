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
package org.apache.hadoop.hbase.crosssite;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator;
import org.apache.hadoop.hbase.filter.ParseConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The utilities of the cross site table.
 */
public class CrossSiteUtil {

  /**
   * Gets the table name in the cluster. The table name looks like tableName_clusterName.
   * 
   * @param tableName
   * @param clusterName
   * @return
   */
  public static String getClusterTableName(String tableName, String clusterName) {
    return tableName + "_" + clusterName;
  }

  /**
   * Gets the table name in a given peer cluster.
   * 
   * @param tableName
   * @param masterCluster
   * @param peerCluster
   * @return table name in a given peer cluster
   */
  // TODO : This does not use the peerCluster?
  public static String getPeerClusterTableName(String tableName, String masterCluster,
      String peerCluster) {
    return getClusterTableName(tableName, masterCluster);
  }

  /**
   * Takes a quoted byte array and converts it into an unquoted byte array For example: given a byte
   * array representing 'abc', it returns a byte array representing abc
   * <p>
   * 
   * @param stringAsByteArray
   *          the quoted byte array
   * @return
   */
  public static byte[] convertByteArrayToString(byte[] stringAsByteArray) {
    if (stringAsByteArray != null && stringAsByteArray.length > 0) {
      int beginWithQuote = 0;
      int endWithQuote = 0;
      if (stringAsByteArray[0] == ParseConstants.SINGLE_QUOTE) {
        beginWithQuote = 1;
      }
      if (stringAsByteArray[stringAsByteArray.length - 1] == ParseConstants.SINGLE_QUOTE) {
        endWithQuote = 1;
      }
      int length = stringAsByteArray.length - beginWithQuote - endWithQuote;
      byte[] targetString = new byte[length];
      Bytes.putBytes(targetString, 0, stringAsByteArray, beginWithQuote, length);
      return targetString;
    }
    return stringAsByteArray;
  }

  /**
   * Converts an int expressed in a byte array to an actual int
   * <p>
   * This doesn't use Bytes.toInt because that assumes that there will be {@link #SIZEOF_INT} bytes
   * available.
   * <p>
   * 
   * @param numberAsByteArray
   *          the int value expressed as a byte array
   * @return the int value
   */
  public static int convertByteArrayToInt(byte[] numberAsByteArray) {

    long tempResult = convertByteArrayToLong(numberAsByteArray);

    if (tempResult > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Integer Argument too large");
    } else if (tempResult < Integer.MIN_VALUE) {
      throw new IllegalArgumentException("Integer Argument too small");
    }

    int result = (int) tempResult;
    return result;
  }

  /**
   * Converts a long expressed in a byte array to an actual long
   * <p>
   * This doesn't use Bytes.toLong because that assumes that there will be {@link #SIZEOF_LONG}
   * bytes available.
   * <p>
   * 
   * @param numberAsByteArray
   *          the long value expressed as a byte array
   * @return the long value
   */
  public static long convertByteArrayToLong(byte[] numberAsByteArray) {
    if (numberAsByteArray == null) {
      throw new IllegalArgumentException("convertByteArrayToLong called with a null array");
    }

    int i = 0;
    long result = 0;
    boolean isNegative = false;

    if (numberAsByteArray[i] == ParseConstants.MINUS_SIGN) {
      i++;
      isNegative = true;
    }

    while (i != numberAsByteArray.length) {
      if (numberAsByteArray[i] < ParseConstants.ZERO || numberAsByteArray[i] > ParseConstants.NINE) {
        throw new IllegalArgumentException("Byte Array should only contain digits");
      }
      result = result * 10 + (numberAsByteArray[i] - ParseConstants.ZERO);
      if (result < 0) {
        throw new IllegalArgumentException("Long Argument too large");
      }
      i++;
    }

    if (isNegative) {
      return -result;
    } else {
      return result;
    }
  }

  /**
   * Reads the split keys from the data.
   * 
   * @param data
   * @return
   * @throws IOException
   */
  public static byte[][] readSplitKeys(byte[] data) throws IOException {
    if (data == null) {
      return null;
    }
    ByteArrayInputStream stream = null;
    try {
      stream = new ByteArrayInputStream(data);
      DataInput in = new DataInputStream(stream);
      if (in.readBoolean()) {
        int size = in.readInt();
        byte[][] splitKeys = new byte[size][];
        for (int i = 0; i < size; i++) {
          splitKeys[i] = Bytes.readByteArray(in);
        }
        return splitKeys;
      } else {
        return null;
      }
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {

        }
      }
    }
  }

  /**
   * Writes the split keys into the DataOutput.
   * 
   * @param splitkeys
   * @param out
   * @throws IOException
   */
  public static void writeSplitKeys(final byte[][] splitkeys, DataOutput out) throws IOException {
    if (splitkeys != null) {
      out.writeBoolean(true);
      int numOfKeys = splitkeys.length;
      out.writeInt(numOfKeys);
      // TODO : this would be too heavy on the zk
      for (int i = 0; i < numOfKeys; i++) {
        Bytes.writeByteArray(out, splitkeys[i]);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  public static ClusterLocator instantiateClusterLocator(Configuration conf,
      String clusterLocatorClassName) throws IOException {
    Class<? extends ClusterLocator> clusterLocatorClass = null;
    try {
      clusterLocatorClass = (Class<? extends ClusterLocator>) conf
          .getClassByName(clusterLocatorClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("Couldn't load clusterlocator class " + clusterLocatorClassName, e);
    }
    if (clusterLocatorClass == null) {
      throw new IOException("Failed loading clusterlocator class " + clusterLocatorClassName);
    }
    try {
      return clusterLocatorClass.asSubclass(ClusterLocator.class).newInstance();
    } catch (Exception e) {
      throw new IOException("Problem loading clusterinf: ", e);
    }
  }

  /**
   * Gets the name of the cross site table by the HTable name.
   * 
   * The HTable name is composed according to the rule, crossSiteTableName_clusterName
   * 
   * @param hTableName
   * @return
   */
  public static String getCrossSiteTableName(String hTableName) {
    int index = hTableName.lastIndexOf('_');
    if (index >= 1) {
      return hTableName.substring(0, index);
    } else {
      throw new IllegalArgumentException("The table name " + hTableName
          + " is in an incorrect format");
    }
  }

  /**
   * Gets the cluster name by the HTable name.
   * 
   * The HTable name is composed according to the rule, crossSiteTableName_clusterName
   * 
   * @param hTableName
   * @return
   */
  public static String getClusterName(String hTableName) {
    int index = hTableName.lastIndexOf('_');
    if (index >= 1 && index < hTableName.length() - 1) {
      return hTableName.substring(index + 1);
    } else {
      throw new IllegalArgumentException("The table name " + hTableName
          + " is in an incorrect format");
    }
  }

  /**
   * Validates the cluster name. The cluster name should not contain _.
   * 
   * If the cluster name contains _, the IllegalArgumentException will be thrown.
   * 
   * @param clusterName
   */
  public static void validateClusterName(String clusterName) {
    if (clusterName.indexOf('_') >= 0) {
      throw new IllegalArgumentException("The cluster name should not contain _, but the name is "
          + clusterName);
    }
  }

  /**
   * Finds out whether to perform the failover when the exception is encounted.
   * 
   * Performs the failover when the exception is not a <code>DoNotRetryIOException</code>.
   * 
   * @param e
   * @return
   */
  public static boolean isFailoverException(Exception e) {
    if (!(e instanceof DoNotRetryIOException)) {
      return true;
    }
    return false;
  }

  /**
   * Gets all the descendant clusters in the hierarchy.
   * 
   * @param hierarchyMap
   * @param parent
   * @return
   */
  public static Set<String> getDescendantClusters(Map<String, Set<String>> hierarchyMap, String parent) {
    Set<String> results = new TreeSet<String>();
    Set<String> children = hierarchyMap.get(parent);
    if (children != null) {
      results.addAll(children);
      for (String child : children) {
        if (!child.equals(parent)) {
          results.addAll(getDescendantClusters(hierarchyMap, child));
        }
      }
    }
    return results;
  }

  /**
   * Gets all the child clusters in the hierarchy.
   * 
   * @param hierarchyMap
   * @param parent
   * @return
   */
  public static Set<String> getChildClusters(Map<String, Set<String>> hierarchyMap, String parent) {
    Set<String> results = null;
    Set<String> children = hierarchyMap.get(parent);
    if (children != null) {
      results = children;
    } else {
      results = Collections.emptySet();
    }
    return results;
  }

  /**
   * Converts the list of ClusterInfo to a list of String.
   * 
   * @param clusterInfos
   * @return
   */
  public static List<String> toStringList(List<ClusterInfo> clusterInfos) {
    List<String> result = Collections.emptyList();
    if (clusterInfos != null && !clusterInfos.isEmpty()) {
      result = new ArrayList<String>();
      for (ClusterInfo clusterInfo : clusterInfos) {
        result.add(clusterInfo.getName());
      }
    }
    return result;
  }
}

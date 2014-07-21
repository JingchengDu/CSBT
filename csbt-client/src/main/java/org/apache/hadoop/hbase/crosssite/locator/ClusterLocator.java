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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Interface for obtaining the cluster name from a given row key.
 */
public interface ClusterLocator extends Writable {

  /**
   * Given the row key it returns the cluster name
   * <p>
   * 
   * @param row
   *          the row key
   * @return located cluster name
   */
  public String getClusterName(final byte[] row) throws RowNotLocatableException;

  /**
   * Given the locator's arguments it constructs the cluster locator
   * <p>
   * 
   * @param locatorArguments
   *          the locator's arguments
   * @return constructed cluster locator object
   */
  public ClusterLocator createClusterLocatorFromArguments(ArrayList<byte[]> locatorArguments);

  /**
   * Generates the split keys for each HTable by the split keys for the cross site big table.
   * 
   * @param clusterName
   * @param splitKeys
   * @return
   * @throws IOException
   */
  public byte[][] getSplitKeys(String clusterName, byte[][] splitKeys) throws IOException;

  /**
   * validate the arguemnts based on the type of the cluster locator
   */
  public void validateArguments(String[] args);

  /**
   * Thrown if the specified cluster locator fails to return the cluster name for the given row key.
   */
  public class RowNotLocatableException extends IOException {

    private static final long serialVersionUID = 2374976924641112006L;

    public RowNotLocatableException() {
      super();
    }

    public RowNotLocatableException(byte[] b) {
      super(Bytes.toString(b));
    }

  }
}

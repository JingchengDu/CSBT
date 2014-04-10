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
package org.apache.hadoop.hbase.client.crosssite.locator;

import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.crosssite.locator.CompositeSubstringClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.PrefixClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.ClusterLocator.RowNotLocatableException;
import org.apache.hadoop.hbase.crosssite.locator.SubstringClusterLocator;
import org.apache.hadoop.hbase.crosssite.locator.SuffixClusterLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestClusterLocators {

  @Test
  public void testPrefixClusterLocator() throws Exception {
    PrefixClusterLocator prefixClusterLocator = new PrefixClusterLocator(",");
    String clusterName = prefixClusterLocator.getClusterName(Bytes.toBytes("hbase1,china"));
    Assert.assertEquals("hbase1", clusterName);
  }

  @Test
  public void testCompositeSubstringClusterLocatorWithInvalidInput() throws Exception {
    CompositeSubstringClusterLocator compositeSubstringClusterLocator = new CompositeSubstringClusterLocator(
        ",", 5);
    try {
      compositeSubstringClusterLocator.getClusterName(Bytes.toBytes("hbase1,china"));
      fail("Should get RowNotLocatableException");
    } catch (RowNotLocatableException e) {

    }
  }

  @Test
  public void testCompositeSubstringClusterLocatorWithIndexZero() throws Exception {
    CompositeSubstringClusterLocator compositeSubstringClusterLocator = new CompositeSubstringClusterLocator(
        ",", 0);
    String clusterName = compositeSubstringClusterLocator.getClusterName(Bytes
        .toBytes("hbase1,china"));
    Assert.assertEquals("hbase1", clusterName);
  }

  @Test
  public void testCompositeSubstringClusterLocatorWithIndexMoreThanZero() throws Exception {
    CompositeSubstringClusterLocator compositeSubstringClusterLocator = new CompositeSubstringClusterLocator(
        ",", 5);
    String clusterName = compositeSubstringClusterLocator.getClusterName(Bytes
        .toBytes("hbase1,china,india,usa,uk,australia"));
    Assert.assertEquals("australia", clusterName);
  }

  @Test
  public void testSubstringClusterLocatorWithValidInputs() throws Exception {
    SubstringClusterLocator substringClusterLocator = new SubstringClusterLocator(1, 7);
    String clusterName = substringClusterLocator.getClusterName(Bytes.toBytes("ahbase1xyz"));
    Assert.assertEquals("hbase1", clusterName);
  }

  @Test
  public void testSuffixClusterLocator() throws Exception {
    SuffixClusterLocator suffixClusterLocator = new SuffixClusterLocator(",");
    String clusterName = suffixClusterLocator.getClusterName(Bytes.toBytes("china,hbase1,hbase2"));
    Assert.assertEquals("hbase2", clusterName);
  }
}

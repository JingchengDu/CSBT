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

/**
 * The constants used in the cross site table.
 */
public class CrossSiteConstants {

  public static final String CROSS_SITE_ZOOKEEPER = "hbase.crosssite.global.zookeeper";

  public static final String CROSS_SITE_TABLE_SCAN_IGNORE_UNAVAILABLE_CLUSTERS = 
      "hbase.crossiste.table.scan.ignore.unavailable.clusters";

  public static final String CROSS_SITE_HTABLE_FACTORY_CLASS = "hbase.crosssite.htable.factory.class";
}

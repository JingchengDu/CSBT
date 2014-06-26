/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.master.RegionState;

/**
 * The cluster status of a cross site table.
 */
public class CrossSiteClusterStatus extends ClusterStatus {

  private static final byte VERSION = 0;

  private List<ClusterStatus> status;

  /**
   * Constructor, for Writable
   */
  public CrossSiteClusterStatus() {
    super();
  }

  public CrossSiteClusterStatus(List<ClusterStatus> status) {
    if (status == null) {
      this.status = Collections.emptyList();
    } else {
      this.status = status;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServerName> getDeadServerNames() {
    List<ServerName> deadServerNames = new ArrayList<ServerName>();
    for (ClusterStatus cs : status) {
      if (cs.getDeadServerNames() != null) {
        for (ServerName sn : cs.getDeadServerNames()) {
          deadServerNames.add(sn);
        }
      }
    }
    return deadServerNames;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getServersSize() {
    int count = 0;
    for (ClusterStatus cs : status) {
      count += cs.getServersSize();
    }
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDeadServers() {
    int count = 0;
    for (ClusterStatus cs : status) {
      count += cs.getDeadServers();
    }
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getAverageLoad() {
    int load = getRegionsCount();
    return (double) load / (double) getServersSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRegionsCount() {
    int count = 0;
    for (ClusterStatus cs : status) {
      count += cs.getRegionsCount();
    }
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRequestsCount() {
    int count = 0;
    for (ClusterStatus cs : status) {
      count += cs.getRequestsCount();
    }
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getHBaseVersion() {
    Set<String> versions = new TreeSet<String>();
    for (ClusterStatus cs : status) {
      versions.add(cs.getHBaseVersion());
    }
    StringBuilder sr = new StringBuilder();
    for (String version : versions) {
      sr.append(version).append(",");
    }
    if (sr.length() > 1) {
      return sr.substring(0, sr.length() - 1);
    } else {
      return sr.toString();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CrossSiteClusterStatus)) {
      return false;
    }
    CrossSiteClusterStatus right = (CrossSiteClusterStatus) o;
    if (this.getClusterStatus().size() != right.getClusterStatus().size()) {
      return false;
    }
    boolean equals = true;
    for (int i = 0; i < this.getClusterStatus().size(); i++) {
      if (!this.getClusterStatus().get(i)
          .equals(right.getClusterStatus().get(i))) {
        equals = false;
        break;
      }
    }
    return equals;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int hashCode = VERSION;
    for (ClusterStatus cs : status) {
      hashCode += cs.hashCode();
    }
    return hashCode;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte getVersion() {
    return VERSION;
  }

  /**
   * @deprecated Use {@link #getServers()}
   */
  @Override
  public Collection<ServerName> getServerInfo() {
    List<ServerName> serverInfos = new ArrayList<ServerName>();
    for (ClusterStatus cs : status) {
      if (cs.getServerInfo() != null) {
        for (ServerName sn : cs.getServerInfo()) {
          serverInfos.add(sn);
        }
      }
    }
    return serverInfos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServerName> getServers() {
    List<ServerName> servers = new ArrayList<ServerName>();
    for (ClusterStatus cs : status) {
      if (cs.getServers() != null) {
        for (ServerName sn : cs.getServers()) {
          servers.add(sn);
        }
      }
    }
    return servers;
  }

  /**
   * Returns the first HMaster in the collection of the ClusterStatus.
   */
  @Override
  public ServerName getMaster() {
    if (status.size() > 1) {
      return status.get(0).getMaster();
    } else {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getBackupMastersSize() {
    int size = 0;
    for (ClusterStatus cs : status) {
      size += cs.getBackupMastersSize();
    }
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServerName> getBackupMasters() {
    List<ServerName> backMasters = new ArrayList<ServerName>();
    for (ClusterStatus cs : status) {
      if (cs.getBackupMasters() != null) {
        for (ServerName sn : cs.getBackupMasters()) {
          backMasters.add(sn);
        }
      }
    }
    return backMasters;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServerLoad getLoad(ServerName sn) {
    ServerLoad load = null;
    for (ClusterStatus cs : status) {
      load = cs.getLoad(sn);
      if (load != null) {
        break;
      }
    }
    return load;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, RegionState> getRegionsInTransition() {
    Map<String, RegionState> state = new HashMap<String, RegionState>();
    for (ClusterStatus cs : status) {
      if (cs.getRegionsInTransition() != null) {
        state.putAll(cs.getRegionsInTransition());
      }
    }
    return state;
  }

  /**
   * Gets the cluster id.
   * This id is composed of the ids of all the sub cluster, looks like
   * id1,id2,id3...
   */
  @Override
  public String getClusterId() {
    StringBuilder sr = new StringBuilder();
    for (ClusterStatus cs : status) {
      sr.append(cs.getClusterId()).append(",");
    }
    if (sr.length() > 1) {
      return sr.substring(0, sr.length() - 1);
    } else {
      return sr.toString();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getMasterCoprocessors() {
    List<String> masterCoprocessors = new ArrayList<String>();
    for (ClusterStatus cs : status) {
      if (cs.getMasterCoprocessors() != null) {
        for (String mcr : cs.getMasterCoprocessors()) {
          masterCoprocessors.add(mcr);
        }
      }
    }
    return masterCoprocessors.toArray(new String[0]);
  }

  /**
   * Gets the list of the status of all the sub clusters.
   * 
   * @return
   */
  public List<ClusterStatus> getClusterStatus() {
    return status;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(status.size());
    for (ClusterStatus cs : status) {
      cs.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    status = new ArrayList<ClusterStatus>(size);
    for (int i = 0; i < size; i++) {
      ClusterStatus cs = new ClusterStatus();
      cs.readFields(in);
      status.add(cs);
    }
  }
}

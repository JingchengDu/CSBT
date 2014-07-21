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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The object of the a cluster.
 */
public class ClusterInfo implements WritableComparable<ClusterInfo> {

  private String name;
  private String address;
  private Set<ClusterInfo> peers = new TreeSet<ClusterInfo>();

  public ClusterInfo() {
  }

  public ClusterInfo(String name, String address, ClusterInfo[] peers) {
    this.name = name;
    this.address = address;
    if (peers != null) {
      List<ClusterInfo> asList = Arrays.asList(peers);
      this.peers = new TreeSet<ClusterInfo>(asList);
    }
  }

  public ClusterInfo(String name, String address, Set<ClusterInfo> peers) {
    this.name = name;
    this.address = address;
    if (peers != null) {
      this.peers.addAll(peers);
    }
  }

  public String getName() {
    return this.name;
  }

  public String getAddress() {
    return this.address;
  }

  public Set<ClusterInfo> getPeers() {
    return this.peers;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public void setPeers(Set<ClusterInfo> peers) {
    this.peers = peers;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, address);
    if (peers != null) {
      out.writeBoolean(true);
      out.writeInt(peers.size());
      for (ClusterInfo peer : peers) {
        peer.write(out);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = Text.readString(in);
    this.address = Text.readString(in);
    if (in.readBoolean()) {
      int size = in.readInt();
      for (int i = 0; i < size; i++) {
        ClusterInfo peer = new ClusterInfo();
        peer.readFields(in);
        peers.add(peer);
      }
    }
  }

  @Override
  public int compareTo(ClusterInfo other) {
    int result = this.name.compareTo(other.name);
    if (result == 0) {
      result = this.address.compareTo(other.address);
    }

    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ClusterInfo))
      return false;

    return compareTo((ClusterInfo) other) == 0;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + address.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[Name : ").append(name).append(", Address : ").append(address).append("]");
    return sb.toString();
  }
}

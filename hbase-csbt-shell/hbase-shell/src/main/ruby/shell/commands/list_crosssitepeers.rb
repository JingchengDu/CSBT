#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Shell
  module Commands
    class ListCrosssitepeers< Command
      def help
        return <<-EOF
List all peer clusters of the current cluster. Optional regular expression parameter could
be used to filter the output. Examples:

  hbase> list_crosssitepeers 'cluster1'
EOF
      end

      def command(cluster)
        now = Time.now
        formatter.header([ "CLUSTER", "CLUSTER_KEY" ])

        list = crosssite_admin.listPeers(cluster)
        list.each do |cluster|
          formatter.row([ cluster.getName(), cluster.getAddress() ])
        end
        formatter.footer(now, list.size)
      end
    end
  end
end
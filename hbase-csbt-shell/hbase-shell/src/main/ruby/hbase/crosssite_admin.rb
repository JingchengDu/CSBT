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
#

include Java
java_import org.apache.hadoop.hbase.util.Pair
java_import org.apache.hadoop.hbase.util.RegionSplitter
java_import org.apache.hadoop.hbase.crosssite.CrossSiteUtil

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class CrossSiteAdmin
    include HBaseConstants
     def initialize(configuration, formatter)
      @crosssiteadmin = org.apache.hadoop.hbase.client.crosssite.CrossSiteHBaseAdmin.new(configuration)
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      @conf = configuration
      @formatter = formatter
     end
    
    #--------------------------------------------------------------------------------
    # adds a cluster to the CSBT
    def addCluster(args={})
      raise(ArgumentError, "Arguments cannot be empty") if args.empty?
      name = args[NAME] if args[NAME]
      raise(ArgumentError, "Name of the cluster should be specified") if name.empty?
      address = args[ADDRESS] if args[ADDRESS]
      raise(ArgumentError, "Address of the cluster should be specified") if address.empty?
      @crosssiteadmin.addCluster(name.to_java.to_s, address.to_java.to_s)
    end
    
    #-----------------------------------------------------------------------------------
    # delete a cluster from the CSBT
    def deleteCluster(cluster)
      raise(ArgumentError, "Clustername cannot be null") if cluster.nil?
      raise(ArgumentError, "Clsutername cannot be empty") if cluster.empty?
      @crosssiteadmin.deleteCluster(cluster.to_java.to_s)
    end
    #-----------------------------------------------------------------------------------
    #adds a peer cluster to an existing cluster
    def addPeer(cluster, args={})
      raise(ArgumentError, "Clustername cannot be empty") if cluster.to_s.empty?
      raise(ArgumentError, "Arguments cannot be empty") if args.empty?
      name = args[NAME] if args[NAME]
      raise(ArgumentError, "Name of the peer should be specified") if name.empty?
      address = args[ADDRESS] if args[ADDRESS]
      raise(ArgumentError, "Address of the peer should be specified") if address.empty?
      peerCluster = Pair.new(name, address)
      @crosssiteadmin.addPeer(cluster, peerCluster)
    end
    
    #----------------------------------------------------------------------------------------------
    # deletes a hierarchy from the csbt node
    def deleteHierarchy(parent)
      raise(ArgumentError, "Parent name cannot be null") if parent.nil?
      raise(ArgumentError, "Parent name cannot be empty") if parent.empty?
      @crosssiteadmin.deleteHierarchy(parent.to_java.to_s)
    end
    #----------------------------------------------------------------------------------------------
    # lists all the child clusters in the hierarchy
    def listChildClusters(parent)
      raise(ArgumentError, "Parent name cannot be null") if parent.nil?
      raise(ArgumentError, "Parent name cannot be empty") if parent.empty?
      @crosssiteadmin.listChildClusters(parent.to_java.to_s)
    end
    #----------------------------------------------------------------------------------------------
    # lists all the descendant clusters in the hierarchy
    def listDescendantClusters(parent)
      raise(ArgumentError, "Parent name cannot be null") if parent.nil?
      raise(ArgumentError, "Parent name cannot be empty") if parent.empty?
      @crosssiteadmin.listDescendantClusters(parent.to_java.to_s)
    end
    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in hbase
    def list(regex = ".*")
      begin
        # Use the old listTables API first for compatibility with older servers
        tabledescriptors = @crosssiteadmin.listTables(regex)
        if tabledescriptors != nil
          tabledescriptors.map { |t| t.getNameAsString }
        end
      rescue => e
        # listTables failed, try the new unprivileged getTableNames API if the cause was
        # an AccessDeniedException
        if e.cause.kind_of? org.apache.hadoop.ipc.RemoteException and e.cause.unwrapRemoteException().kind_of? org.apache.hadoop.hbase.security.AccessDeniedException
          @crosssiteadmin.getTableNames(regex)
        else
          # Not an access control failure, re-raise
          raise e
        end
      end
    end
    
    #----------------------------------------------------------------------------------------------
    # Returns a list of clusters in hbase
    def listCluster()
      @crosssiteadmin.listClusters()
    end
    
    #---------------------------------------------------------------------------------------------
    # returns a list of peers in hbase
    def listPeers(cluster)
      @crosssiteadmin.listPeers(cluster.to_s.to_java)
    end
    #----------------------------------------------------------------------------------------------
    
    #----------------------------------------------------------------------------------------------
    # adds hierarchy
    def addHierarchy(cluster, *args)
      raise(ArgumentError, "Cluster name cannot be null") if cluster.nil?
      raise(ArgumentError, "Children should be string array") unless args.kind_of?(Array)
      if args.kind_of?(Array)
        children = [ args ].flatten.compact
      end
      begin
        @crosssiteadmin.addHierarchy(cluster, children.to_java(:string))
      end
    end
    
    #-----------------------------------------------------------------------------------------------
    # deletes peers from the cluster
    def deletePeers(cluster, args={})
      raise(ArgumentError, "Cluster name cannot be null") if cluster.nil?
      peers = args[PEERS] if args[PEERS]
      if peers != nil
        raise(ArgumentError, "Peers should be string array") unless peers.kind_of?(Array)
        peers = [peers].flatten.compact
        @crosssiteadmin.deletePeers(cluster, peers.to_java(:string))
      else 
        @crosssiteadmin.deletePeers(cluster)	
      end
    end
    #----------------------------------------------------------------------------------------------     
    # Creates a table
    def create(table_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, "Table name must be of type String") unless table_name.kind_of?(String)

      # Flatten params array
      args = args.flatten.compact

      # Fail if no column families defined
      raise(ArgumentError, "Table must have at least one column family") if args.empty?
      # Start defining the table
      htd = org.apache.hadoop.hbase.HTableDescriptor.new(table_name)
      splits = nil
      clusterLocator = nil
      # Args are either columns or splits, add them to the table definition
      # TODO: add table options support
      args.each do |arg|
        unless arg.kind_of?(String) || arg.kind_of?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
        end
        if arg.kind_of?(String)
          # the arg is a string, default action is to add a column to the table
          htd.addFamily(hcd(arg, htd))
        else
          # arg is a hash.  4 possibilities:
          if (arg.has_key?(SPLITS) or arg.has_key?(SPLITS_FILE))
            if arg.has_key?(SPLITS_FILE)
              unless File.exist?(arg[SPLITS_FILE])
                raise(ArgumentError, "Splits file #{arg[SPLITS_FILE]} doesn't exist")
              end
              arg[SPLITS] = []
              File.foreach(arg[SPLITS_FILE]) do |line|
                arg[SPLITS].push(line.strip())
              end
            end

            splits = Java::byte[][arg[SPLITS].size].new
            idx = 0
            arg[SPLITS].each do |split|
              splits[idx] = split.to_java_bytes
              idx = idx + 1
            end
          elsif (arg.has_key?(NUMREGIONS) or arg.has_key?(SPLITALGO))
            # (1) deprecated region pre-split API
            raise(ArgumentError, "Column family configuration should be specified in a separate clause") if arg.has_key?(NAME)
            raise(ArgumentError, "Number of regions must be specified") unless arg.has_key?(NUMREGIONS)
            raise(ArgumentError, "Split algorithm must be specified") unless arg.has_key?(SPLITALGO)
            raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
            num_regions = arg[NUMREGIONS]
            split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
            splits = split_algo.split(JInteger.valueOf(num_regions))
          elsif (method = arg.delete(METHOD))
            # (2) table_att modification
            raise(ArgumentError, "table_att is currently the only supported method") unless method == 'table_att'
            raise(ArgumentError, "NUMREGIONS & SPLITALGO must both be specified") unless arg.has_key?(NUMREGIONS) == arg.has_key?(split_algo)
            htd.setMaxFileSize(JLong.valueOf(arg[MAX_FILESIZE])) if arg[MAX_FILESIZE]
            htd.setReadOnly(JBoolean.valueOf(arg[READONLY])) if arg[READONLY]
            htd.setMemStoreFlushSize(JLong.valueOf(arg[MEMSTORE_FLUSHSIZE])) if arg[MEMSTORE_FLUSHSIZE]
            htd.setDeferredLogFlush(JBoolean.valueOf(arg[DEFERRED_LOG_FLUSH])) if arg[DEFERRED_LOG_FLUSH]
            htd.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT]) if arg[COMPRESSION_COMPACT]
            if arg[NUMREGIONS]
              raise(ArgumentError, "Number of regions must be greater than 1") unless arg[NUMREGIONS] > 1
              num_regions = arg[NUMREGIONS]
              split_algo = RegionSplitter.newSplitAlgoInstance(@conf, arg[SPLITALGO])
              splits = split_algo.split(JInteger.valueOf(num_regions))
            end
            if arg[CONFIGURATION]
              raise(ArgumentError, "#{CONFIGURATION} must be a Hash type") unless arg.kind_of?(Hash)
              for k,v in arg[CONFIGURATION]
                v = v.to_s unless v.nil?
                htd.setValue(k, v)
              end
            end
          elsif (arg.has_key?(LOCATOR))
            clusterLocator = CrossSiteUtil.instantiateClusterLocator(@conf, arg[LOCATOR])
            raise(ArgumentError, "Clsuter locator must be specified") if clusterLocator.nil?
            if (arg.has_key?(LOCATOR_ARGS))
             clusterLocatorArgs = arg[LOCATOR_ARGS]
             # TODO : Check the arg specification for the PrefixClusterLocator
             clusterLocator.validateArguments(clusterLocatorArgs.to_java(:string))
            end
          #end  
          else
            # (3) column family spec
            descriptor = hcd(arg, htd)
            htd.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT]) if arg[COMPRESSION_COMPACT]
            htd.addFamily(hcd(arg, htd))
          end
        end
      end

      if splits.nil?
        # Perform the create table call
        @crosssiteadmin.createTable(htd)
      else
        # Perform the create table call
        if clusterLocator.nil?
          @crosssiteadmin.createTable(htd, splits)
        else
          @crosssiteadmin.createTable(htd, splits, clusterLocator, true)
        end
      end
    end
    #--------------------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------
    # Return a new HColumnDescriptor made of passed args
    def hcd(arg, htd)
      # String arg, single parameter constructor
      return org.apache.hadoop.hbase.HColumnDescriptor.new(arg) if arg.kind_of?(String)

      raise(ArgumentError, "Column family #{arg} must have a name") unless name = arg[NAME]

      family = htd.getFamily(name.to_java_bytes)
      # create it if it's a new family
      family ||= org.apache.hadoop.hbase.HColumnDescriptor.new(name.to_java_bytes)

      family.setBlockCacheEnabled(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKCACHE)
      family.setScope(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::REPLICATION_SCOPE)
      family.setInMemory(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::IN_MEMORY)
      family.setTimeToLive(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::TTL])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::TTL)
      family.setDataBlockEncoding(org.apache.hadoop.hbase.io.encoding.DataBlockEncoding.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::DATA_BLOCK_ENCODING)
      family.setEncodeOnDisk(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::ENCODE_ON_DISK)
      family.setBlocksize(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOCKSIZE)
      family.setMaxVersions(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::VERSIONS)
      family.setMinVersions(JInteger.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::MIN_VERSIONS)
      family.setKeepDeletedCells(JBoolean.valueOf(arg[org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS])) if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::KEEP_DELETED_CELLS)
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER)
        bloomtype = arg[org.apache.hadoop.hbase.HColumnDescriptor::BLOOMFILTER].upcase
        unless org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.include?(bloomtype)      
          raise(ArgumentError, "BloomFilter type #{bloomtype} is not supported. Use one of " + org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.constants.join(" ")) 
        else 
          family.setBloomFilterType(org.apache.hadoop.hbase.regionserver.StoreFile::BloomType.valueOf(bloomtype))
        end
      end
      if arg.include?(org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION)
        compression = arg[org.apache.hadoop.hbase.HColumnDescriptor::COMPRESSION].upcase
        unless org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.constants.include?(compression)      
          raise(ArgumentError, "Compression #{compression} is not supported. Use one of " + org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.constants.join(" ")) 
        else 
          family.setCompressionType(org.apache.hadoop.hbase.io.hfile.Compression::Algorithm.valueOf(compression))
        end
      end

      if arg[CONFIGURATION]
        raise(ArgumentError, "#{CONFIGURATION} must be a Hash type") unless arg.kind_of?(Hash)
        for k,v in arg[CONFIGURATION]
          v = v.to_s unless v.nil?
          family.setValue(k, v)
        end
      end
      return family
    end
    #----------------------------------------------------------------------------------------------     
    # Disables a table
    def disable(table_name)
      #Do we need to do some other checks here
      @crosssiteadmin.disableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------     
    # Enables a table
    def enable(table_name)
      #Do we need to do some other checks here
      @crosssiteadmin.enableTable(table_name)
    end

    #-------------------------------------------------------------------------------------------------
    #Deletes a table
    def deleteTable(table_name)
      #Do we need to do some other checks here
      @crosssiteadmin.deleteTable(table_name)
    end
  end
end
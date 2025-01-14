/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveTableContext;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-06 14:18
 **/
public class DefaultHiveTableContext implements IHiveTableContext {
    public final Table table;
    public final HiveTable hiveTable;
    public final String database;
    public final String tableName;
    public final IMetaStoreClient storeClient;

    public DefaultHiveTableContext(HiveTable hiveTable, Table table, String database, String tableName, IMetaStoreClient storeClient) {
        this.hiveTable = Objects.requireNonNull(hiveTable, "hiveTable can not be null");
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.database = Objects.requireNonNull(database, "database can not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName can not be null");
        this.storeClient = Objects.requireNonNull(storeClient, "storeClient can no tbe null");
    }

    public List<Partition> listPartitions(short maxPtsCount) {
        try {
            return this.storeClient.listPartitions(database, tableName, maxPtsCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public List<Partition> listPartitionsByFilter(String criteria, short maxPtsCount) {
        try {
            return storeClient.listPartitionsByFilter(database, tableName, criteria, maxPtsCount);
        } catch (TException e) {
            throw new RuntimeException("database:" + database + ", tableName:"
                    + this.tableName + ", criteria:" + criteria + ", maxPtsCount:" + maxPtsCount, e);
        }
    }


}

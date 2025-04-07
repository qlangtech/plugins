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

package com.qlangtech.tis.hive.reader;

import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.datax.hdfs.HdfsTDFSSession;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 15:46
 **/
public class HiveDFSSession extends HdfsTDFSSession {
    private final HiveDFSLinker dfsLinker;

    public HiveDFSSession(String rootPath, Supplier<FileSystemFactory> fsFactorySuppier, HiveDFSLinker dfsLinker) {
        super(rootPath, fsFactorySuppier);
        this.dfsLinker = dfsLinker;
    }

    public List<ColumnMetaData> parseColsMeta(String tabName) {
        Hiveserver2DataSourceFactory dsFactory = dfsLinker.getDataSourceFactory();
        return dsFactory.getTableMetadata(false, null, EntityName.parse(tabName));

//        HiveMeta metadata = dsFactory.metadata;
//        HiveTable table = metadata.createMetaStoreClient().getTable(dsFactory.dbName, tabName);
//
//        return table.getSchema();
    }
//    public HiveDFSSession(HiveDFSLinker dfsLinker) {
//
//        this.dfsLinker = dfsLinker;
//    }
}

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

package com.alibaba.datax.plugin.reader.hive;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ftpreader.Key;
import com.alibaba.datax.plugin.writer.tdfsreader.TDFSReader;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.DataXHiveReader;
import com.qlangtech.tis.hive.reader.HiveDFSLinker;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class HiveReader extends TDFSReader {

    public static class Job extends TDFSReader.Job {

        /**
         * 获取一个独立PT下的所有相关文件
         * @param cfg
         * @param dfsSession
         * @param entityName
         * @return
         */
        @Override
        protected List<String> getReaderPaths(Configuration cfg, ITDFSSession dfsSession, Optional<String> entityName) {

            HiveDFSLinker dfsLinker = (HiveDFSLinker) this.getDFSLinker();
            Hiveserver2DataSourceFactory dsFactory = dfsLinker.getDataSourceFactory();
            HiveTable hiveTable = dsFactory.metadata.createMetaStoreClient() //
                    .getTable(dsFactory.dbName, entityName.orElseThrow(() -> new IllegalArgumentException("entityName can not be null")));
            Optional<String> ptFilter = Optional.of(((DataXHiveReader) this.getReader()).ptFilter);
            // Optional<String> ptFilter = Optional.of("pt=" + HiveTable.KEY_PT_LATEST);
            List<String> pts = hiveTable.listPartitions(ptFilter);

            List<String> result = pts.stream() //
                    .map((pt) -> IPath.pathConcat(hiveTable.getStorageLocation(), pt)).collect(Collectors.toList());

            cfg.set(Key.PATH, result);
            return result;
        }
    }

    public static class Task extends TDFSReader.Task {

    }
}

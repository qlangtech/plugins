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

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.plugin.datax.resmatcher.BasicDFSResMatcher;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 22:12
 **/
public class HiveDFSResMatcher extends BasicDFSResMatcher {
    //private final DataXHiveReader hiveReader;

    public HiveDFSResMatcher() {
        //  this.hiveReader = hiveReader;
        this.maxTraversalLevel = 2;
    }

    /**
     * 获取已经选中的表的cols
     * @param pluginContext
     * @param pipelineName
     * @param dfsReader
     * @param table
     * @return
     * @throws TableNotFoundException
     */
    @Override
    public List<ColumnMetaData> getTableMetadata(
            IPluginContext pluginContext, String pipelineName, IDFSReader dfsReader, EntityName table) throws TableNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRDBMSSupport() {
        return true;
    }

    @Override
    public SourceColsMeta getSourceColsMeta(ITDFSSession session, Optional<String> entitName, String path, IDataxProcessor processor) {

        final HiveDFSSession hiveSession = (HiveDFSSession) session;
        String tabName = entitName.orElseThrow(() -> new IllegalStateException("param entityName must be present"));
        return getSourceColsMeta(processor, tabName, hiveSession.parseColsMeta(tabName));
    }

    @Override
    public List<ISelectedTab> getSelectedTabs(IDFSReader dfsReader) {
        return ((Supplier<List<ISelectedTab>>) dfsReader).get();
    }


    @Override
    public boolean hasMulitTable(IDFSReader dfsReader) {
        return false;
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter, IDFSReader dfsReader) {
        return new HiveGroupChildTaskIterator(dfsReader.getSelectedTabs());
    }

    @Override
    public boolean isMatch(ITDFSSession.Res testRes) {
        return true;
    }
}

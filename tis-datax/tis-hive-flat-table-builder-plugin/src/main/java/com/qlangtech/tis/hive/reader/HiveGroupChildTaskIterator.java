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

import com.beust.jcommander.internal.Lists;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.plugin.ds.ISelectedTab;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class HiveGroupChildTaskIterator implements IGroupChildTaskIterator {

    private final AtomicInteger taskIndex = new AtomicInteger(0);
    ConcurrentHashMap<String, List<DataXCfgGenerator.DBDataXChildTask>> groupedInfo = new ConcurrentHashMap();
    private final Iterator<ISelectedTab> tabs;

    private ISelectedTab current;

    public HiveGroupChildTaskIterator(List<ISelectedTab> tabs) {
        this.tabs = tabs.iterator();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        if (tabs.hasNext()) {
            current = tabs.next();
            return true;
        } else {
            current = null;
            return false;
        }
    }

    @Override
    public Map<String, List<DataXCfgGenerator.DBDataXChildTask>> getGroupedInfo() {
        return groupedInfo;


        //   return IGroupChildTaskIterator.super.getGroupedInfo();
    }

    @Override
    public IDataxReaderContext next() {

        HiveDataxReaderContext readerContext = new HiveDataxReaderContext(current, taskIndex.getAndIncrement());
        List<DataXCfgGenerator.DBDataXChildTask> childTasks = groupedInfo.computeIfAbsent(current.getName(),
                (tabName) -> {
            return Lists.newArrayList();
        });
        childTasks.add(new DataXCfgGenerator.DBDataXChildTask(readerContext.getReaderContextId(),
                readerContext.getReaderContextId(), readerContext.getTaskName()));
        return readerContext;
    }


    private static class HiveDataxReaderContext implements IDataxReaderContext {
        protected final ISelectedTab tab;
        private final Integer taskIndex;

        public HiveDataxReaderContext(ISelectedTab tab, Integer taskIndex) {
            this.tab = Objects.requireNonNull(tab, "selected table can not be null");
            this.taskIndex = taskIndex;
        }

        @Override
        public String getReaderContextId() {
            // throw new UnsupportedOperationException();
            return "hive";
        }

        @Override
        public String getTaskName() {
            return this.tab.getName() + "_" + this.taskIndex;
        }

        @Override
        public String getSourceEntityName() {
            return this.tab.getName();
        }

        @Override
        public String getSourceTableName() {
            return this.tab.getName();
        }
    }
}

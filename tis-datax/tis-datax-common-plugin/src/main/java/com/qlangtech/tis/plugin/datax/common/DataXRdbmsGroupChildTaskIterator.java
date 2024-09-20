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

package com.qlangtech.tis.plugin.datax.common;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-02-03 21:17
 **/
public class DataXRdbmsGroupChildTaskIterator implements IGroupChildTaskIterator {
    AtomicInteger selectedTabIndex = new AtomicInteger(0);
    AtomicInteger taskIndex = new AtomicInteger(0);
    ConcurrentHashMap<String, List<DBDataXChildTask>> groupedInfo = new ConcurrentHashMap();
    private final List<SelectedTab> tabs;
    final int selectedTabsSize;
    AtomicReference<Iterator<IDataSourceDumper>> dumperItRef = new AtomicReference<>();
    private final FilterUnexistCol filterUnexistCol;
    private final BasicDataXRdbmsReader rdbmsReader;

    @Override
    public Map<String, List<DBDataXChildTask>> getGroupedInfo() {
        return groupedInfo;
    }

    public DataXRdbmsGroupChildTaskIterator(BasicDataXRdbmsReader rdbmsReader, FilterUnexistCol filterUnexistCol,
                                            List<SelectedTab> tabs) {
        this.rdbmsReader = rdbmsReader;
        this.tabs = tabs;
        this.selectedTabsSize = tabs.size();
        this.filterUnexistCol = filterUnexistCol;
    }

    @Override
    public boolean hasNext() {

        Iterator<IDataSourceDumper> dumperIt = initDataSourceDumperIterator();
        if (dumperIt.hasNext()) {
            return true;
        } else {
            if (selectedTabIndex.get() >= selectedTabsSize) {
                return false;
            } else {
                dumperItRef.set(null);
                initDataSourceDumperIterator();
                return true;
            }
        }
    }

    private Iterator<IDataSourceDumper> initDataSourceDumperIterator() {
        Iterator<IDataSourceDumper> dumperIt;
        if ((dumperIt = dumperItRef.get()) == null) {
            SelectedTab tab = tabs.get(selectedTabIndex.getAndIncrement());
            if (StringUtils.isEmpty(tab.getName())) {
                throw new IllegalStateException("tableName can not be null");
            }
            //                    List<ColumnMetaData> tableMetadata = null;
            //                    IDataSourceDumper dumper = null;
            DataDumpers dataDumpers = null;
            TISTable tisTab = new TISTable();
            tisTab.setTableName(tab.getName());
            //            int[] index = {0};
            //            tisTab.setReflectCols(tab.getCols().stream().map((c) -> {
            //                return createColumnMetaData(index, c.getName());
            //            }).collect(Collectors.toList()));

            dataDumpers = this.rdbmsReader.getDataSourceFactory().getDataDumpers(tisTab);
            dumperIt = dataDumpers.dumpers;
            dumperItRef.set(dumperIt);
        }
        return dumperIt;

    }

    @Override
    public IDataxReaderContext next() {
        Iterator<IDataSourceDumper> dumperIterator = dumperItRef.get();
        Objects.requireNonNull(dumperIterator,
                "dumperIterator can not be null,selectedTabIndex:" + selectedTabIndex.get());
        IDataSourceDumper dumper = dumperIterator.next();
        SelectedTab tab = tabs.get(selectedTabIndex.get() - 1);
        String childTask = tab.getName() + "_" + taskIndex.getAndIncrement();
        List<DBDataXChildTask> childTasks = groupedInfo.computeIfAbsent(tab.getName(),
                (tabname) -> Lists.newArrayList());
        childTasks.add(new DBDataXChildTask(dumper.getDbHost(),
                this.rdbmsReader.getDataSourceFactory().identityValue(), childTask));
        RdbmsReaderContext dataxContext = rdbmsReader.createDataXReaderContext(childTask, tab, dumper);

        dataxContext.setWhere(tab.getWhere());

        dataxContext.setCols(filterUnexistCol.getCols(tab));


        return dataxContext;
    }

    @Override
    public void close() throws IOException {
        try {
            this.filterUnexistCol.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

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

package com.qlangtech.tis.plugin.datax.test;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.*;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 测试用的mock对象
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-23 10:18
 **/
public class TestSelectedTabs {
    public static final String tabNameOrderDetail = "orderdetail";
    public static final String tabNameTotalpayinfo = "totalpayinfo";
    public static List<ColumnMetaData> tabColsMetaOrderDetail = Lists.newArrayList(new ColumnMetaData(0, "col1",
            new DataType(JDBCTypes.VARCHAR), true, false), new ColumnMetaData(1, "col2",
            new DataType(JDBCTypes.VARCHAR), false, true), new ColumnMetaData(2, "col3",
            new DataType(JDBCTypes.VARCHAR), false, true), new ColumnMetaData(3, "col4",
            new DataType(JDBCTypes.VARCHAR), false, true));
    public static List<ColumnMetaData> tabColsMetaTotalpayinfo = Lists.newArrayList(new ColumnMetaData(0, "col1",
            new DataType(JDBCTypes.VARCHAR), true, false), new ColumnMetaData(1, "col2",
            new DataType(JDBCTypes.VARCHAR), false, true), new ColumnMetaData(2, "col3",
            new DataType(JDBCTypes.VARCHAR), false, true), new ColumnMetaData(3, "col4",
            new DataType(JDBCTypes.VARCHAR), false, true));

    public static List<SelectedTab> createSelectedTabs() {
        return createSelectedTabs(Integer.MAX_VALUE);
    }

    public static List<SelectedTab> createSelectedTabs(int count) {
        try {
            return createSelectedTabs(count, SelectedTab.class, Optional.empty(), (tab) -> {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends SelectedTab> List<T> createSelectedTabs(int count, Class<T> clazz,
                                                                     Optional<CMeta.ElementCreatorFactory> elementCreator, Consumer<T> postProcess) {


        try {
            List<T> selectedTabs = Lists.newArrayList();
            T selectedTab = clazz.newInstance();
            selectedTab.cols = Lists.newArrayList( //
                    CMeta.create(elementCreator, "col1", JDBCTypes.VARCHAR) //
                    , CMeta.create(elementCreator, "col2", JDBCTypes.BIGINT) //
                    , CMeta.create(elementCreator, "col3", JDBCTypes.TIMESTAMP));// .setCols

            selectedTab.setWhere("delete = 0");
            selectedTab.name = tabNameOrderDetail;
            postProcess.accept(selectedTab);
            selectedTabs.add(selectedTab);

            if (count > 1) {
                selectedTab = clazz.newInstance();
                selectedTab.cols = Lists.newArrayList( //
                        CMeta.create(elementCreator, "col1", JDBCTypes.VARCHAR) //
                        , CMeta.create(elementCreator, "col2", JDBCTypes.BIGINT) //
                        , CMeta.create(elementCreator, "col3", JDBCTypes.TIMESTAMP) //
                        , CMeta.create(elementCreator, "col4", JDBCTypes.TIMESTAMP));
                // selectedTab.setCols(Lists.newArrayList("col1", "col2", "col3", "col4"));
                selectedTab.setWhere("delete = 0");
                selectedTab.name = tabNameTotalpayinfo;
                postProcess.accept(selectedTab);
                selectedTabs.add(selectedTab);
            }
            return selectedTabs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<IDataxProcessor.TableMap> createTableMapper() {
        IDataxProcessor.TableMap tm =
                new IDataxProcessor.TableMap(Lists.newArrayList("col1", "col2", "col3").stream().map((c) -> {
            CMeta meta = new CMeta();
            meta.setName(c);
            meta.setType(DataXReaderColType.STRING.dataType);
            return meta;
        }).collect(Collectors.toList()));
        tm.setFrom("orderinfo");
        tm.setTo("orderinfo_new");

        Optional<IDataxProcessor.TableMap> tableMap = Optional.of(tm);
        return tableMap;
    }
}

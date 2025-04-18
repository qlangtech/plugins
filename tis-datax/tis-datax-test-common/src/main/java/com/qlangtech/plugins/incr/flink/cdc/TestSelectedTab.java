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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-09-28 16:23
 **/
public class TestSelectedTab extends SelectedTab {
    // final List<CMeta> colsMeta;

    public static SelectedTab load(TemporaryFolder folder, Class<?> clazz, String xmlFileName) throws Exception {
        File selTabs = folder.newFile(xmlFileName);
        IOUtils.loadResourceFromClasspath(clazz
                , xmlFileName, true, (input) -> {
                    FileUtils.copyInputStreamToFile(input, selTabs);
                    return null;
                });

        PluginStore<SelectedTab> tabsStore = new PluginStore<>(SelectedTab.class, new XmlFile(selTabs));
        return Objects.requireNonNull(tabsStore.getPlugin(), "select tab can not be null");
    }

    public static SelectedTab createSelectedTab(EntityName tabName
            , DataSourceFactory dataSourceFactory) throws TableNotFoundException {
        return createSelectedTab(tabName, dataSourceFactory, (tab) -> {
            try {
                return dataSourceFactory.getTableMetadata(false, null, tab);
            } catch (TableNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static SelectedTab createSelectedTab(EntityName tabName
            , DataSourceFactory dataSourceFactory, Function<EntityName, List<ColumnMetaData>> tableMetadataGetter) {
        return createSelectedTab(tabName, dataSourceFactory, tableMetadataGetter, (t) -> {
        }, () -> new CMeta());
    }

    public static SelectedTab createSelectedTab(EntityName tabName //
            , DataSourceFactory dataSourceFactory
            , Function<EntityName, List<ColumnMetaData>> tableMetadataGetter
            , Consumer<SelectedTab> baseTabSetter, Supplier<CMeta> cmetaCreator) {
        List<ColumnMetaData> tableMetadata = tableMetadataGetter.apply(tabName);// dataSourceFactory.getTableMetadata(false, tabName);
        if (CollectionUtils.isEmpty(tableMetadata)) {
            throw new IllegalStateException("tabName:" + tabName + " relevant can not be empty");
        }
        List<String> pks = Lists.newArrayList();
        List<CMeta> colsMeta = tableMetadata.stream().map((col) -> {
            if (col.isPk()) {
                pks.add(col.getName());
            }
            CMeta c = cmetaCreator.get();
            c.setPk(col.isPk());
            c.setName(col.getName());
            c.setNullable(col.isNullable());
            c.setType(col.getType());
            c.setComment(col.getComment());
            return c;
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pks)) {
            throw new IllegalStateException("pks can not be empty");
        }
        TestSelectedTab baseTab = new TestSelectedTab(tabName.getTableName(), colsMeta);
        baseTab.primaryKeys = pks;
        baseTab.setCols(tableMetadata.stream().map((m) -> m.getName()).collect(Collectors.toList()));
        baseTabSetter.accept(baseTab);

        return baseTab;
    }

    public TestSelectedTab(String name, List<CMeta> colsMeta) {
        super(name);
        this.cols.addAll(colsMeta);
    }

    @Override
    public void setCols(List<String> cols) {
        //  super.setCols(cols);
    }
//    @Override
//    public List<CMeta> getCols() {
//        return colsMeta;
//    }
}

/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax.common;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.Memoizer;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 09:54
 **/
public abstract class BasicDataXRdbmsReader extends DataxReader {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 99, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    @SubForm(desClazz = SelectedTab.class
            , idListGetScript = "return com.qlangtech.tis.coredefine.module.action.DataxAction.getTablesInDB(filter);", atLeastOne = true)
    public List<SelectedTab> selectedTabs;

    private transient boolean colTypeSetted;

    @Override
    public final List<SelectedTab> getSelectedTabs() {

        if (this.colTypeSetted) {
            return selectedTabs;
        }

        try {
            Memoizer<String, Map<String, ColumnMetaData>> tabsMeta = getTabsMeta();
            return this.selectedTabs.stream().map((tab) -> {
                Map<String, ColumnMetaData> colsMeta = tabsMeta.get(tab.getName());
                ColumnMetaData colMeta = null;
                for (ISelectedTab.ColMeta col : tab.getCols()) {
                    colMeta = colsMeta.get(col.getName());
                    if (colMeta == null) {
                        throw new IllegalStateException("col:" + col.getName() + " can not find relevant 'ColumnMetaData'");
                    }
                    col.setType(ISelectedTab.DataXReaderColType.parse(colMeta.getType()));
                }
                return tab;
            }).collect(Collectors.toList());
        } finally {
            this.colTypeSetted = true;
        }
    }


    @Override
    public final Iterator<IDataxReaderContext> getSubTasks() {
        Objects.requireNonNull(this.selectedTabs, "selectedTabs can not be null");
        DataSourceFactory dsFactory = this.getDataSourceFactory();

        Memoizer<String, Map<String, ColumnMetaData>> tabColsMap = getTabsMeta();

        AtomicInteger selectedTabIndex = new AtomicInteger(0);
        AtomicInteger taskIndex = new AtomicInteger(0);

        final int selectedTabsSize = this.selectedTabs.size();

        AtomicReference<Iterator<IDataSourceDumper>> dumperItRef = new AtomicReference<>();

        return new Iterator<IDataxReaderContext>() {
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
                    SelectedTab tab = selectedTabs.get(selectedTabIndex.getAndIncrement());
                    if (StringUtils.isEmpty(tab.getName())) {
                        throw new IllegalStateException("tableName can not be null");
                    }
//                    List<ColumnMetaData> tableMetadata = null;
//                    IDataSourceDumper dumper = null;
                    DataDumpers dataDumpers = null;
                    TISTable tisTab = new TISTable();
                    tisTab.setTableName(tab.getName());

                    dataDumpers = dsFactory.getDataDumpers(tisTab);
                    dumperIt = dataDumpers.dumpers;
                    dumperItRef.set(dumperIt);
                }
                return dumperIt;
            }

            @Override
            public IDataxReaderContext next() {
                Iterator<IDataSourceDumper> dumperIterator = dumperItRef.get();
                Objects.requireNonNull(dumperIterator, "dumperIterator can not be null,selectedTabIndex:" + selectedTabIndex.get());
                IDataSourceDumper dumper = dumperIterator.next();
                SelectedTab tab = selectedTabs.get(selectedTabIndex.get() - 1);

                RdbmsReaderContext dataxContext = createDataXReaderContext(tab.getName() + "_" + taskIndex.getAndIncrement(), tab, dumper, dsFactory);

//                MySQLDataXReaderContext dataxContext = new MySQLDataXReaderContext(
//                        tab.getName() + "_" + taskIndex.getAndIncrement(), tab.getName());
//                dataxContext.setJdbcUrl(dumper.getDbHost());
//                dataxContext.setUsername(dsFactory.getUserName());
//                dataxContext.setPassword(dsFactory.getPassword());
                dataxContext.setWhere(tab.getWhere());

                Map<String, ColumnMetaData> tableMetadata = tabColsMap.get(tab.getName());
                if (tab.isAllCols()) {
                    dataxContext.setCols(tableMetadata.keySet().stream().collect(Collectors.toList()));
                } else {
                    dataxContext.setCols(tab.cols.stream().filter((c) -> tableMetadata.containsKey(c)).collect(Collectors.toList()));
//                    dataxContext.cols = tableMetadata.values().stream().filter((col) -> {
//                        return tab.containCol(col.getKey());
//                    }).map((t) -> t.getValue()).collect(Collectors.toList());
                }

                return dataxContext;
            }
        };
    }

    protected abstract RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper, DataSourceFactory dsFactory);

//        MySQLDataSourceFactory myDsFactory = (MySQLDataSourceFactory) dsFactory;
//        MySQLDataxContext mysqlContext = new MySQLDataxContext();
//        MySQLDataXReaderContext dataxContext = new MySQLDataXReaderContext(jobName, tab.getName(), mysqlContext);
//
//        mysqlContext.setJdbcUrl(dumper.getDbHost());
//        mysqlContext.setUsername(myDsFactory.getUserName());
//        mysqlContext.setPassword(myDsFactory.getPassword());
//
//        return dataxContext;
//    }

    private Memoizer<String, Map<String, ColumnMetaData>> getTabsMeta() {
        return new Memoizer<String, Map<String, ColumnMetaData>>() {
            @Override
            public Map<String, ColumnMetaData> compute(String tab) {

                DataSourceFactory datasource = getDataSourceFactory();
                Objects.requireNonNull(datasource, "ds:" + dbName + " relevant DataSource can not be find");

                return datasource.getTableMetadata(tab)
                        .stream().collect(Collectors.toMap((m) -> m.getKey(), (m) -> m));
            }
        };
    }


    @Override
    public final String getTemplate() {
        return template;
    }

    public final void setSelectedTabs(List<SelectedTab> selectedTabs) {
        this.selectedTabs = selectedTabs;
    }

    @Override
    public final List<String> getTablesInDB() {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTablesInDB();
    }

    protected DataSourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return dsStore.getPlugin();
    }

    @Override
    public final List<ColumnMetaData> getTableMetadata(String table) {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTableMetadata(table);
    }


    public static abstract class BasicDataXRdbmsReaderDescriptor extends DataxReader.BaseDataxReaderDescriptor implements FormFieldType.IMultiSelectValidator, SubForm.ISubFormItemValidate {
        public BasicDataXRdbmsReaderDescriptor() {
            super();
        }

        @Override
        public final boolean isRdbms() {
            return true;
        }

        @Override
        public boolean validateSubFormItems(IControlMsgHandler msgHandler, Context context
                , SuFormProperties props, IPropertyType.SubFormFilter filter, Map<String, JSONObject> formData) {

            Integer maxReaderTabCount = Integer.MAX_VALUE;
            try {
                maxReaderTabCount = Integer.parseInt(filter.uploadPluginMeta.getExtraParam(ESTableAlias.MAX_READER_TABLE_SELECT_COUNT));
            } catch (Throwable e) {

            }

            if (formData.size() > maxReaderTabCount) {
                msgHandler.addErrorMessage(context, "导入表不能超过" + maxReaderTabCount + "张");
                return false;
            }

            return true;
        }

        @Override
        public boolean validate(IFieldErrorHandler msgHandler, Optional<IPropertyType.SubFormFilter> subFormFilter
                , Context context, String fieldName, List<FormFieldType.SelectedItem> items) {

            return true;
        }


    }
}

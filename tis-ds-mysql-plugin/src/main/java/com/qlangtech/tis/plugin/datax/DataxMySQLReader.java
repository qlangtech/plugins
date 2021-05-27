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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.Memoizer;
import org.apache.commons.lang.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataxMySQLReader extends DataxReader {
    private static final String DATAX_NAME = "MySQL";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require, Validator.identity})
    public boolean splitPk;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    @SubForm(desClazz = SelectedTab.class
            , idListGetScript = "return com.qlangtech.tis.coredefine.module.action.DataxAction.getTablesInDB(filter);", atLeastOne = true)
    public List<SelectedTab> selectedTabs;

    @Override
    public List<SelectedTab> getSelectedTabs() {
        return this.selectedTabs;
    }

//    @Override
//    public boolean hasMulitTable() {
//        return getSelectedTabs().size() > 1;
//    }


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxMySQLReader.class, "mysql-reader-tpl.json");
    }


    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        Objects.requireNonNull(this.selectedTabs, "selectedTabs can not be null");
        MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) this.getDataSourceFactory();

        Memoizer<String, List<ColumnMetaData>> tabColsMap = new Memoizer<String, List<ColumnMetaData>>() {
            @Override
            public List<ColumnMetaData> compute(String tab) {
                return dsFactory.getTableMetadata(tab);
            }
        };

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
                MySQLDataXReaderContext dataxContext = new MySQLDataXReaderContext(tab.getName() + "_" + taskIndex.getAndIncrement(), tab.getName());
                dataxContext.jdbcUrl = dumper.getDbHost();
                dataxContext.tabName = tab.getName();
                dataxContext.username = dsFactory.getUserName();
                dataxContext.password = dsFactory.getPassword();
                dataxContext.setWhere(tab.getWhere());

                List<ColumnMetaData> tableMetadata = tabColsMap.get(tab.getName());
                if (tab.isAllCols()) {
                    dataxContext.cols = tableMetadata.stream().map((t) -> t.getValue()).collect(Collectors.toList());
                } else {
                    dataxContext.cols = tableMetadata.stream().filter((col) -> {
                        return tab.containCol(col.getKey());
                    }).map((t) -> t.getValue()).collect(Collectors.toList());
                }

                return dataxContext;
            }
        };
    }


    @Override
    public String getTemplate() {
        return template;
    }

    public void setSelectedTabs(List<SelectedTab> selectedTabs) {
        this.selectedTabs = selectedTabs;
    }

    @Override
    public List<String> getTablesInDB() {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTablesInDB();
    }

    protected DataSourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return dsStore.getPlugin();
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(String table) {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTableMetadata(table);
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor implements FormFieldType.IMultiSelectValidator {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        public boolean validate(IFieldErrorHandler msgHandler, Context context, String fieldName, List<FormFieldType.SelectedItem> items) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.SubFormFilter;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 09:54
 **/
public abstract class BasicDataXRdbmsReader<DS extends DataSourceFactory> extends DataxReader
        implements IDataSourceFactoryGetter, KeyedPluginStore.IPluginKeyAware, AfterPluginSaved {

    private static final Logger logger = LoggerFactory.getLogger(BasicDataXRdbmsReader.class);
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 98, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer fetchSize;

    @FormField(ordinal = 99, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    @SubForm(desClazz = SelectedTab.class //
            , idListGetScript = "return com.qlangtech.tis.coredefine.module.action.DataxAction.getTablesInDB(filter);", atLeastOne = true)
    public transient List<SelectedTab> selectedTabs;


    private transient int preSelectedTabsHash;
    public String dataXName;

    public Integer getRowFetchSize() {
        return this.fetchSize;
    }

//    @Override
//    public Map<String, ContextParamConfig> getDBContextParams() {
//        return ContextParamConfig.defaultContextParams();
//        ContextParamConfig dbName = new ContextParamConfig("dbName") {
//            @Override
//            public ContextParamValGetter<RdbmsRunningContext> valGetter() {
//                return new DbNameContextParamValGetter();
//            }
//
//            @Override
//            public DataType getDataType() {
//                return DataType.createVarChar(50);
//            }
//        };
//
//        ContextParamConfig sysTimestamp = new ContextParamConfig("timestamp") {
//            @Override
//            public ContextParamValGetter<RdbmsRunningContext> valGetter() {
//                return new SystemTimeStampContextParamValGetter();
//            }
//
//            @Override
//            public DataType getDataType() {
//                return DataType.getType(JDBCTypes.TIMESTAMP);
//            }
//        };
//
//        ContextParamConfig tableName = new ContextParamConfig("tableName") {
//            @Override
//            public ContextParamValGetter<RdbmsRunningContext> valGetter() {
//                return new TableNameContextParamValGetter();
//            }
//
//            @Override
//            public DataType getDataType() {
//                return DataType.createVarChar(50);
//            }
//        };
//
//        return Lists.newArrayList(dbName, tableName, sysTimestamp)
//                .stream().collect(Collectors.toMap((cfg) -> cfg.getKeyName(), (cfg) -> cfg));
//
////        dbContextParams.put(dbName.getKeyName(), dbName);
////        return dbContextParams;
    //  }

    @Override
    public final void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this.preSelectedTabsHash = -1;
    }

    @Override
    public final List<SelectedTab> getSelectedTabs() {

        if (selectedTabs == null) {
            return Collections.emptyList();
        }

        if (this.preSelectedTabsHash == selectedTabs.hashCode()) {
            return selectedTabs;
        }
        this.selectedTabs = fillSelectedTabMeta(false, this.selectedTabs);
        this.preSelectedTabsHash = selectedTabs.hashCode();
        return this.selectedTabs;

    }

    @Override
    public List<SelectedTab> fillSelectedTabMeta(List<SelectedTab> tabs) {
        return fillSelectedTabMeta(true, tabs);
    }

    /**
     * @param forceFill 忽视缓存的存在，每次都填充
     * @param tabs
     * @return
     */
    public List<SelectedTab> fillSelectedTabMeta(boolean forceFill, List<SelectedTab> tabs) {
        boolean shallFillSelectedTabMeta = forceFill || shallFillSelectedTabMeta();

        if (shallFillSelectedTabMeta) {
            try (TableColsMeta tabsMeta = getTabsMeta()) {
                return tabs.stream().map((tab) -> {
                    ColumnMetaData.fillSelectedTabMeta(tab, (t) -> {
                        return tabsMeta.get(t.getName());
                    });
                    return tab;
                }).collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return tabs;
    }

    protected boolean shallFillSelectedTabMeta() {
        if (CollectionUtils.isEmpty(this.selectedTabs)) {
            return true;
        }
        for (SelectedTab tab : this.selectedTabs) {
            for (CMeta c : tab.cols) {
                return (c.getType() == null);
            }
        }
        return true;
    }

    protected abstract RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab,
                                                                   IDataSourceDumper dumper);

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }


    @Override
    public final IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
        Objects.requireNonNull(this.selectedTabs, "selectedTabs can not be null");
        List<SelectedTab> tabs = this.selectedTabs.stream().filter(filter).collect(Collectors.toList());

        return new DataXRdbmsGroupChildTaskIterator(this, this.getUnexistColFilter(), tabs);
    }


    protected FilterUnexistCol getUnexistColFilter() {
        return FilterUnexistCol.noneFilter();
    }

    TableColsMeta getTabsMeta() {
        return new TableColsMeta(getDataSourceFactory(), this.dbName);
    }

    @Override
    public final String getTemplate() {
        return template;
    }

    public final void setSelectedTabs(List<SelectedTab> selectedTabs) {
        this.selectedTabs = selectedTabs;
    }

    @Override
    public final TableInDB getTablesInDB() {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTablesInDB();
    }

    @Override
    public final void refresh() {
        getDataSourceFactory().refresh();
    }

    @Override
    public void startScanDependency() {
        this.getDataSourceFactory();
    }

    @Override
    public DS getDataSourceFactory() {
        return TIS.getDataBasePlugin(PostedDSProp.parse(this.dbName));
    }

    public final List<ColumnMetaData> getTableMetadata(EntityName table) throws TableNotFoundException {
        return this.getTableMetadata(false, table);
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) throws TableNotFoundException {
        DataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTableMetadata(inSink, table);
    }


    @Override
    protected Class<BasicDataXRdbmsReaderDescriptor> getExpectDescClass() {
        return BasicDataXRdbmsReaderDescriptor.class;
    }

    public static abstract class BasicDataXRdbmsReaderDescriptor extends DataxReader.BaseDataxReaderDescriptor implements FormFieldType.IMultiSelectValidator {
        public BasicDataXRdbmsReaderDescriptor() {
            super();
        }

        @Override
        public final boolean isRdbms() {
            return true;
        }

        public boolean validateFetchSize(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                         String value) {
            try {
                int fetchSize = Integer.parseInt(value);
                if (fetchSize < 1) {
                    msgHandler.addFieldError(context, fieldName, "不能小于1");
                }
                if (fetchSize > 2048) {
                    msgHandler.addFieldError(context, fieldName, "不能大于2048,以免进程OOM");
                    return false;
                }
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
            }
            return true;
        }

        public boolean validateDbName(IFieldErrorHandler msgHandler, Context context, String fieldName, String dbName) {
            DataSourceFactory ds = TIS.getDataBasePlugin(PostedDSProp.parse(dbName), false);
            if (ds == null) {
                msgHandler.addFieldError(context, fieldName, "请确认该数据源是否存在");
                return false;
            }
            return true;
        }


        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            try {
//                ParseDescribable<Describable> readerDescribable = this.newInstance((IPluginContext) msgHandler,
//                        postFormVals.rawFormData, Optional.empty());
                BasicDataXRdbmsReader rdbmsReader = postFormVals.newInstance();// readerDescribable.getInstance();
                rdbmsReader.getTablesInDB();
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
                // msgHandler.addErrorMessage(context, );
                msgHandler.addFieldError(context, BasicDataXRdbmsWriter.KEY_DB_NAME_FIELD_NAME,
                        "数据源连接不正常," + TisException.getErrMsg(e));
                return false;
            }

            return true;
        }

        @Override
        public boolean validate(IFieldErrorHandler msgHandler, Optional<SubFormFilter> subFormFilter,
                                Context context, String fieldName, List<FormFieldType.SelectedItem> items) {

            return true;
        }


    }
}

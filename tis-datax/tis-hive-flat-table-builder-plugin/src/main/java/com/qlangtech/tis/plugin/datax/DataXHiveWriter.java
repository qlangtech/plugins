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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fs.ITableBuildTask;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilder;
import com.qlangtech.tis.fullbuild.taskflow.ITemplateContext;
import com.qlangtech.tis.fullbuild.taskflow.hive.JoinHiveTask;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:48
 * @see com.qlangtech.tis.plugin.datax.TisDataXHiveWriter
 **/
@Public
public class DataXHiveWriter extends BasicFSWriter implements IFlatTableBuilder, IDataSourceFactoryGetter {
    private static final String DATAX_NAME = "Hive";

    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String hiveConn;


    //    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
//    public String hiveConn;
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer partitionRetainNum;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tabPrefix;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public String partitionFormat;

    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    @Override
    public String getTemplate() {
        return this.template;
    }

    public MREngine getEngineType() {
        return MREngine.HIVE;
    }

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new HiveDataXContext("tishivewriter", tableMap, this.dataXName);
    }

    /**
     * ========================================================
     * implements: IFlatTableBuilder
     *
     * @see IFlatTableBuilder
     */
    @Override
    public void startTask(ITableBuildTask dumpTask) {

    }

    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode, ITemplateContext tplContext
            , ITaskContext taskContext, IJoinTaskStatus joinTaskStatus, IFlatTableBuilder flatTableBuilder
            , IDataSourceFactoryGetter dsGetter, IPrimaryTabFinder primaryTabFinder) {
//        ISqlTask nodeMeta, boolean isFinalNode, IPrimaryTabFinder erRules, IJoinTaskStatus joinTaskStatus
//                , ITISFileSystem fileSystem, MREngine mrEngine, IDataSourceFactoryGetter dsFactoryGetter

        JoinHiveTask joinHiveTask = new JoinHiveTask(nodeMeta, isFinalNode, primaryTabFinder, joinTaskStatus, this.getFs().getFileSystem(), MREngine.HIVE, dsGetter);
        return joinHiveTask;
    }

    /**
     * END implements: IFlatTableBuilder
     * ==================================================================
     */
    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHiveWriter.class, "DataXHiveWriter-tpl.json");
    }

    public Connection getConnection() {
        Hiveserver2DataSourceFactory dsFactory = getDataSourceFactory();
        String jdbcUrl = dsFactory.getJdbcUrl();
        try {
            return dsFactory.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException(jdbcUrl, e);
        }
    }

    @Override
    public Hiveserver2DataSourceFactory getDataSourceFactory() {
        if (StringUtils.isBlank(this.hiveConn)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return BasicDataXRdbmsWriter.getDs(this.hiveConn);
    }

    @Override
    public Integer getRowFetchSize() {
        throw new UnsupportedOperationException();
    }

    public IHiveConnGetter getHiveConnGetter() {
        return getDataSourceFactory();
    }

    public class HiveDataXContext extends FSDataXContext {

        private final String dataxPluginName;

        public HiveDataXContext(String dataxPluginName, IDataxProcessor.TableMap tabMap, String dataXName) {
            super(tabMap, dataXName);
            this.dataxPluginName = dataxPluginName;
        }

        @Override
        public String getTableName() {
            return StringUtils.trimToEmpty(tabPrefix) + super.getTableName();
        }

        public String getDataxPluginName() {
            return this.dataxPluginName;
        }

        public Integer getPartitionRetainNum() {
            return partitionRetainNum;
        }

        public String getPartitionFormat() {
            return partitionFormat;
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends DataXHdfsWriter.DefaultDescriptor {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME_HIVE_CONN, () -> ParamsConfig.getItems(IHiveConnGetter.PLUGIN_NAME));
        }

        public boolean validatePartitionRetainNum(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer retainNum = Integer.parseInt(value);
            if (retainNum < 1 || retainNum > 5) {
                msgHandler.addFieldError(context, fieldName, "数目必须为不小于1且不大于5之间");
                return false;
            }
            return true;
        }

//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return HiveFlatTableBuilder.validateHiveAvailable(msgHandler, context, postFormVals);
//        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

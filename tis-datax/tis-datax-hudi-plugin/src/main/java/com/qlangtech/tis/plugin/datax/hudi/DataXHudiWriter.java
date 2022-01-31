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

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import com.qlangtech.tis.plugin.datax.DataXHdfsWriter;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-21 13:02
 **/
@Public
public class DataXHudiWriter extends BasicFSWriter implements KeyedPluginStore.IPluginKeyAware {
    public static final String DATAX_NAME = "Hudi";

    public static final String KEY_FIELD_NAME_SPARK_CONN = "sparkConn";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sparkConn;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String hiveConn;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String tabType;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String partitionedBy;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public boolean autoCreateTable;


    @FormField(ordinal = 100, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    /**
     * @return
     */
    public static List<Option> allPrimaryKeys(Object contextObj) {
        List<Option> pks = Lists.newArrayList();
        pks.add(new Option("base_id"));
        return pks;
    }

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new FSDataXContext(tableMap, dataXName);
    }

    public IHiveConnGetter getHiveConnGetter() {
        return ParamsConfig.getItem(this.hiveConn, IHiveConnGetter.PLUGIN_NAME);
    }

    public ISparkConnGetter getSparkConnGetter() {
        return ISparkConnGetter.getConnGetter(this.sparkConn);
    }

    public Connection getConnection() {
        try {
            ParamsConfig connGetter = (ParamsConfig) getHiveConnGetter();
            return connGetter.createConfigInstance();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private String dataXName;

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public String getTemplate() {
        return template;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHudiWriter.class, "DataXHudiWriter-tpl.json");
    }

    @TISExtension()
    public static class DefaultDescriptor extends DataXHdfsWriter.DefaultDescriptor implements IRewriteSuFormProperties {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME_SPARK_CONN, () -> ParamsConfig.getItems(ISparkConnGetter.PLUGIN_NAME));
            this.registerSelectOptions(BasicFSWriter.KEY_FIELD_NAME_HIVE_CONN, () -> ParamsConfig.getItems(IHiveConnGetter.PLUGIN_NAME));
        }

        @Override
        public SuFormProperties overwriteSubPluginFormPropertyTypes(SuFormProperties subformProps) throws Exception {
            String overwriteSubField = IOUtils.loadResourceFromClasspath(DataXHudiWriter.class
                    , DataXHudiWriter.class.getSimpleName() + "." + subformProps.getSubFormFieldName() + ".json", true);
            JSONObject subField = JSON.parseObject(overwriteSubField);
            Class<?> clazz = DataXHudiWriter.class.getClassLoader().loadClass(subField.getString(SubForm.FIELD_DES_CLASS));
            return SuFormProperties.copy(filterFieldProp(Descriptor.buildPropertyTypes(this, clazz)), subformProps);
        }

        @Override
        public SuFormProperties.SuFormPropertiesBehaviorMeta overwriteBehaviorMeta(
                SuFormProperties.SuFormPropertiesBehaviorMeta behaviorMeta) throws Exception {


//            {
//                "clickBtnLabel": "设置",
//                    "onClickFillData": {
//                "cols": {
//                    "method": "getTableMetadata",
//                    "params": ["id"]
//                }
//            }
//            }

            Map<String, SuFormProperties.SuFormPropertyGetterMeta> onClickFillData = behaviorMeta.getOnClickFillData();
            //  JSONObject onClickFillData = behaviorMeta.getJSONObject("onClickFillData");
            SuFormProperties.SuFormPropertyGetterMeta propProcess = new SuFormProperties.SuFormPropertyGetterMeta();
            propProcess.setMethod("getPrimaryKeys");
            propProcess.setParams(Collections.singletonList("id"));
//            propProcess.put("method", "getPrimaryKeys");
//            JSONArray params = new JSONArray();
//            params.add("id");
//            propProcess.put("params", params);
            onClickFillData.put(HudiSelectedTab.KEY_RECORD_FIELD, propProcess);


            return behaviorMeta;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

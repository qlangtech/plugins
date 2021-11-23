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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXMongodbWriter extends DataxWriter
        implements IDataxProcessor.INullTableMapCreator, KeyedPluginStore.IPluginKeyAware {
    private static final Logger logger = LoggerFactory.getLogger(DataXMongodbWriter.class);
    private static final String KEY_FIELD_UPSERT_INFO = "upsertInfo";
    private static final String KEY_FIELD_COLUMN = "column";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String collectionName;
    @FormField(ordinal = 4, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;

    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {})
    public String upsertInfo;

    @FormField(ordinal = 11, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public String dataXName;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXMongodbWriter.class, "DataXMongodbWriter-tpl.json");
    }

    public MangoDBDataSourceFactory getDsFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return (MangoDBDataSourceFactory) dsStore.getPlugin();
    }


    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    /**
     * 取得默认的列内容
     *
     * @return
     */
    public static String getDftColumn() {
//[{"name":"user_id","type":"string"},{"name":"user_name","type":"array","splitter":","}]

        JSONArray fields = new JSONArray();

        DataxReader dataReader = DataxReader.getThreadBingDataXReader();
        if (dataReader == null) {
            return "[]";
        }

        List<ISelectedTab> selectedTabs = dataReader.getSelectedTabs();
        if (CollectionUtils.isEmpty(selectedTabs)) {
            return "[]";
        }
        for (ISelectedTab tab : selectedTabs) {
            tab.getCols().forEach((col) -> {
                JSONObject field = new JSONObject();
                field.put("name", col.getName());
                field.put("type", col.getType().getCollapse().getLiteria());
                fields.add(field);
            });

            break;
        }

        return JsonUtil.toString(fields);
    }

//    public static String getDftCollectionName() {
//        DataxReader dataReader = DataxReader.getThreadBingDataXReader();
//        if (dataReader == null) {
//            return StringUtils.EMPTY;
//        }
//
//        try {
//            List<ISelectedTab> selectedTabs = dataReader.getSelectedTabs();
//            for (ISelectedTab tab : selectedTabs) {
//                return tab.getName();
//            }
//        } catch (Throwable e) {
//            logger.warn(dataReader.getDescriptor().getDisplayName(), e);
//        }
//
//        return StringUtils.EMPTY;
//    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must not be present");
        }
        MongoDBWriterContext context = new MongoDBWriterContext(this);
        return context;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXMongodbReader.validateColumnContent(msgHandler, context, fieldName, value);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            JSONArray cols = JSON.parseArray(postFormVals.getField(KEY_FIELD_COLUMN));
            JSONObject col = null;
            try {
                String upsertinfo = postFormVals.getField(KEY_FIELD_UPSERT_INFO);
                if (StringUtils.isNotBlank(upsertinfo)) {

                    JSONObject info = JSON.parseObject(upsertinfo);
                    // isUpsert":true,"upsertKey
                    Boolean isUpsert = info.getBoolean("isUpsert");
                    if (isUpsert == null && isUpsert) {
                        String upsertKey = info.getString("upsertKey");
                        if (StringUtils.isEmpty(upsertinfo)) {
                            msgHandler.addFieldError(context, KEY_FIELD_UPSERT_INFO, "属性'upsertKey'必须填写");
                            return false;
                        }
                        boolean findField = false;
                        for (int i = 0; i < cols.size(); i++) {
                            col = cols.getJSONObject(i);
                            if (StringUtils.equals(upsertKey, col.getString("name"))) {
                                findField = true;
                            }
                        }

                        if (!findField) {
                            msgHandler.addFieldError(context, KEY_FIELD_UPSERT_INFO
                                    , "属性'upsertKey':" + upsertinfo + "在" + KEY_FIELD_COLUMN + "没有找到");
                            return false;
                        }
                    }
                }


            } catch (Throwable e) {
                msgHandler.addFieldError(context, KEY_FIELD_UPSERT_INFO, e.getMessage());
                return false;
            }

            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return verify(msgHandler, context, postFormVals);
        }

        @Override
        public boolean isSupportMultiTable() {
            return false;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DataXMongodbReader.DATAX_NAME;
        }
    }
}

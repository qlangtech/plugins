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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.BaseSubFormProperties;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.datax.mongo.MongoColumnMetaData;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTabExtend;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.plugin.incr.ISelectedTabExtendFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.impl.AttrVals;
import org.apache.commons.lang.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * https://gitee.com/mirrors/DataX/blob/master/mongodbreader/doc/mongodbreader.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader
 **/
@Public
public class DataXMongodbReader extends BasicDataXRdbmsReader<MangoDBDataSourceFactory> {

    public static final String DATAX_NAME = "MongoDB";
    public static final String TYPE_ARRAY = "array";
    public static final Set<String> acceptTypes = Sets.newHashSet("int", "long", "double", "string", TYPE_ARRAY,
            "date", "boolean", "bytes");

    private static final Logger logger = LoggerFactory.getLogger(DataXMongodbReader.class);

    //    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    //    public String dbName;
    //    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    //    public String collectionName;
    //    @FormField(ordinal = 4, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    //    public String column;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer inspectRowCount;


    //    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    //    public String query;

    //    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    //    public String template;

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) throws TableNotFoundException {
        MangoDBDataSourceFactory plugin = getDataSourceFactory();
        Map<String, MongoColumnMetaData> colsSchema = Maps.newHashMap();
        try {
            MongoClient mongoClient = Objects.requireNonNull(plugin.unwrap(MongoClient.class), " mongoClient can not "
                    + "be null ");

            MongoDatabase database = mongoClient.getDatabase(plugin.getDbName());
            MongoCollection<Document> user = database.getCollection(table.getTableName());
            BsonValue val = null;
            BsonDocument bdoc = null;
            MongoColumnMetaData colMeta = null;

            for (Document doc : user.find().limit(Objects.requireNonNull(inspectRowCount,
                    "inspectRowCount can not " + "be" + " null"))) {

                bdoc = doc.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

                MongoColumnMetaData.parseMongoDocTypes(false, Collections.emptyList(), colsSchema, bdoc);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        List<ColumnMetaData> result = MongoColumnMetaData.reorder(colsSchema);
        return result;
    }

    /**
     * end implements: DBConfigGetter
     */
    //    public MangoDBDataSourceFactory getDsFactory() {
    //        return TIS.getDataBasePlugin(PostedDSProp.parse(this.dbName));
    //        //   return (MangoDBDataSourceFactory) dsStore.getPlugin();
    //    }
    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXMongodbReader.class, "DataXMongodbReader-tpl.json");
    }

    //    @Override
    //    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
    //        IDataxReaderContext readerContext = new MongoDBReaderContext(this.collectionName, this);
    //        //return Collections.singleton(readerContext).iterator();
    //        return IGroupChildTaskIterator.create(readerContext);
    //    }

    //    @Override
    //    public List<ISelectedTab> getSelectedTabs() {
    //        if (StringUtils.isEmpty(this.collectionName)) {
    //            throw new IllegalStateException("property collectionName can not be empty");
    //        }
    //        MongoDBTable tab = new MongoDBTable(this.collectionName);
    //
    //
    //        List<ColCfg> cols = JSON.parseArray(this.column, ColCfg.class);
    //        tab.cols = cols.stream().map((c) -> {
    //            CMeta colMeta = new CMeta();
    //            colMeta.setName(c.getName());
    //            colMeta.setType(convertType(c.getType()));
    //            return colMeta;
    //        }).collect(Collectors.toList());
    //
    //        return Collections.singletonList(tab);
    //    }

    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        return null;
    }

    private DataType convertType(String type) {
        if (!acceptTypes.contains(type)) {
            throw new IllegalArgumentException("illegal type:" + type);
        }
        switch (type) {
            case "int":
            case "long":
                return DataXReaderColType.Long.dataType;
            case "double":
                return DataXReaderColType.Double.dataType;
            case "string":
            case "array":
                return DataXReaderColType.STRING.dataType;
            case "date":
                return DataXReaderColType.Date.dataType;
            case "boolean":
                return DataXReaderColType.Boolean.dataType;
            case "bytes":
                return DataXReaderColType.Bytes.dataType;
            default:
                throw new IllegalStateException("illegal type:" + type);
        }
    }


    public static class ColCfg {
        private String name;
        private String type;
        private String splitter;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getSplitter() {
            return splitter;
        }

        public void setSplitter(String splitter) {
            this.splitter = splitter;
        }
    }


    //    @Override
    //    public String getTemplate() {
    //        return this.template;
    //    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor implements ISelectedTabExtendFactory, SubForm.ISubFormItemValidate {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateInspectRowCount(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                               String value) {

            Integer inspectRowCount = Integer.parseInt(value);
            if (inspectRowCount < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0的整数");
                return false;
            }

            final int maxInspectRowCount = 1000;
            if (inspectRowCount > maxInspectRowCount) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + maxInspectRowCount + "的整数");
                return false;
            }
            //  return validateColumnContent(msgHandler, context, fieldName, value);

            return true;
        }

        @Override
        public boolean validateSubFormItems(IControlMsgHandler msgHandler, Context context,
                                            BaseSubFormProperties props, IPropertyType.SubFormFilter subFormFilter,
                                            AttrVals formData) {
            // 校验一次提交的全部selectForm
            return true;
        }


        @Override
        public boolean validateSubForm(IControlMsgHandler msgHandler, Context context, SelectedTab tab) {

            MongoCMeta mongoCMeta = null;


            String jsonPath = null;
            String name = null;
            DataType type = null;
            int colIndex = 0;

            final String nameKey = "name";

            for (CMeta cmeta : tab.cols) {
                mongoCMeta = (MongoCMeta) cmeta;

                int docSplitFieldIndex = 0;
                for (MongoCMeta.MongoDocSplitCMeta splitCMeta : mongoCMeta.getDocFieldSplitMetas()) {

                    String fieldJsonPathKey = joinField(SelectedTab.KEY_FIELD_COLS, Lists.newArrayList(colIndex,
                            docSplitFieldIndex), "jsonPath");

                    jsonPath = splitCMeta.getJsonPath();

                    if (Validator.require.validate(msgHandler, context, fieldJsonPathKey, jsonPath)) {

                        Validator.validatePattern(msgHandler, context,
                                Validator.rule(MongoCMeta.MongoDocSplitCMeta.PATTERN_JSON_PATH, "需由带点字段名组成"),
                                fieldJsonPathKey, jsonPath);

                    }

                    name = splitCMeta.getName();
                    final String fieldNameKey = joinField(SelectedTab.KEY_FIELD_COLS, Lists.newArrayList(colIndex,
                            docSplitFieldIndex), nameKey);

                    if (Validator.require.validate(msgHandler, context, fieldNameKey, name) //
                            && Validator.db_col_name.validate(msgHandler, context, fieldNameKey, name)) {

                    }

                    type = splitCMeta.getType();

                    docSplitFieldIndex++;
                }

                colIndex++;
            }

            if (context.hasErrors()) {
                return false;
            }

            // 校验字段是否有重复
            AtomicInteger keyIndex = new AtomicInteger();
            Map<String, List<Integer>> existKeys = tab.cols.stream().collect(Collectors.toMap((c) -> c.getName(),
                    (c) -> Collections.singletonList(keyIndex.getAndIncrement())));
            List<Integer> fieldIndex = null;
            colIndex = 0;
            for (CMeta cmeta : tab.cols) {
                mongoCMeta = (MongoCMeta) cmeta;
                int docSplitFieldIndex = 0;
                for (MongoCMeta.MongoDocSplitCMeta splitCMeta : mongoCMeta.getDocFieldSplitMetas()) {

                    name = splitCMeta.getName();

                    if ((fieldIndex = existKeys.get(name)) != null) {

                        msgHandler.addFieldError(context, joinField(SelectedTab.KEY_FIELD_COLS, fieldIndex, nameKey),
                                "字段重复");

                        msgHandler.addFieldError(context, joinField(SelectedTab.KEY_FIELD_COLS,
                                Lists.newArrayList(colIndex, docSplitFieldIndex), nameKey), "字段重复");

                        return false;
                    } else {
                        existKeys.put(name, Lists.newArrayList(colIndex, docSplitFieldIndex));
                    }

                    docSplitFieldIndex++;
                }
                colIndex++;
            }


            return true;
        }

        private String joinField(String fkey, Object... keys) {

            List<String> joins = Lists.newArrayList(fkey);
            for (Object key : keys) {
                if (key instanceof String) {
                    joins.add(".");
                    joins.add((String) key);
                } else if (key instanceof List) {
                    joins.add((String) ((List) key).stream().map((k) -> "[" + k + "]").collect(Collectors.joining()));
                } else {
                    throw new IllegalStateException("illegal type:" + key);
                }
            }

            return joins.stream().collect(Collectors.joining());
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.verify(msgHandler, context, postFormVals);
        }

        @Override
        public final Descriptor<SelectedTabExtend> getSelectedTableExtendDescriptor() {
            return TIS.get().getDescriptor(MongoSelectedTabExtend.class);
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.MongoDB;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }

    @Deprecated
    public static boolean validateColumnContent(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                                String value) {
        try {
            JSONObject col = null;
            String attrName = null;
            String attrType = null;
            JSONArray cols = JSON.parseArray(value);
            if (cols.size() < 1) {
                msgHandler.addFieldError(context, fieldName, "请填写列配置信息");
                return false;
            }
            for (int i = 0; i < cols.size(); i++) {
                col = cols.getJSONObject(i);
                if ((attrName = col.getString("name")) == null) {
                    msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列缺少name属性");
                    return false;
                } else {
                    Matcher matcher = ValidatorCommons.PATTERN_DB_COL_NAME.matcher(attrName);
                    if (!matcher.matches()) {
                        msgHandler.addFieldError(context, fieldName,
                                "第" + (i + 1) + "个列name属性" + ValidatorCommons.MSG_DB_COL_NAME_ERROR);
                        return false;
                    }
                }
                if ((attrType = col.getString("type")) == null) {
                    msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列缺少'type'属性");
                    return false;
                } else {
                    attrType = StringUtils.lowerCase(attrType);
                    if (!acceptTypes.contains(attrType)) {
                        msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列'type'属性不合规则");
                        return false;
                    }
                    if (TYPE_ARRAY.equals(attrType)) {
                        String spliter = col.getString("splitter");
                        if (StringUtils.isBlank(spliter)) {
                            msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列'type'为'" + TYPE_ARRAY +
                                    "'的属性必须有'splitter'属性");
                            return false;
                        }
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            msgHandler.addFieldError(context, fieldName, "JsonArray的格式有误");
            return false;
        }

        return true;
    }
}

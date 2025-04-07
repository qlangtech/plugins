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
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.reader.mongodbreader.util.IMongoTable;
import com.alibaba.datax.plugin.reader.mongodbreader.util.IMongoTableFinder;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.SubFormFilter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.BaseSubFormProperties;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.CompanionPluginFactory;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.FilterUnexistCol;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMetaCreatorFactory;
import com.qlangtech.tis.plugin.datax.mongo.MongoDataXColUtils;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTabExtend;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import com.qlangtech.tis.util.impl.AttrVals;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler.joinField;

/**
 * https://gitee.com/mirrors/DataX/blob/master/mongodbreader/doc/mongodbreader.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader
 * @see com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader.Task
 **/
@Public
public class DataXMongodbReader extends BasicDataXRdbmsReader<MangoDBDataSourceFactory> implements IMongoTableFinder {

    public static final String DATAX_NAME = "MongoDB";
    public static final String TYPE_ARRAY = "array";
    public static final String TYPE_INT = "int";
    public static final Set<String> acceptTypes
            = Sets.newHashSet(TYPE_INT, "long", "double", "string", TYPE_ARRAY, "date", "boolean", "bytes");

    private static final Logger logger = LoggerFactory.getLogger(DataXMongodbReader.class);

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String timeZone;

    public ZoneId parseZoneId() {
        return ZoneId.of(timeZone);
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, IPluginContext pluginContext, EntityName table) throws TableNotFoundException {
        MangoDBDataSourceFactory plugin = getDataSourceFactory();
        return plugin.getTableMetadata(inSink, pluginContext, table);
    }

    @Override
    public IMongoTable findMongoTable(String tableName) {
        for (SelectedTab tab : this.selectedTabs) {
            if (StringUtils.equals(tableName, tab.getName())) {
                return new DefaultMongoTable(tab, parseZoneId());
            }
        }
        throw new IllegalStateException("can not find table:" + tableName + " relevant selected tablek");
    }

    @Override
    protected FilterUnexistCol getUnexistColFilter() {

        return new FilterUnexistCol() {
            @Override
            public List<String> getCols(SelectedTab tab) {
                DefaultMongoTable mongoTable = (DefaultMongoTable) findMongoTable(tab.getName());
                List<Pair<MongoCMeta, Function<BsonDocument, Column>>> cols = mongoTable.getMongoPresentCols();
                return cols.stream().map((p) -> p.getKey().getName()).collect(Collectors.toList());
            }

            @Override
            public void close() throws Exception {

            }
        };
    }

    static class DefaultMongoTable implements IMongoTable {
        private final SelectedTab table;
        private final MongoSelectedTabExtend tabExtend;
        private List<Pair<MongoCMeta, Function<BsonDocument, Column>>> presentCols;
        private final ZoneId zoneId;

        public DefaultMongoTable(SelectedTab table, ZoneId zoneId) {
            this.table = table;
            this.tabExtend = Objects.requireNonNull((MongoSelectedTabExtend) table.getSourceProps(),
                    "source table:" + table.getName() + " relevant extend props can not be null");
            this.zoneId = Objects.requireNonNull(zoneId, "zoneId");
        }

        @Override
        public Record convert2RecordByItem(Record record, BsonDocument item) {

            for (Pair<MongoCMeta, Function<BsonDocument, Column>> p : this.getMongoPresentCols()) {
                record.addColumn(p.getValue().apply(item));
            }

            return record;
        }


        List<Pair<MongoCMeta, Function<BsonDocument, Column>>> getMongoPresentCols() {
            if (this.presentCols == null) {
                // List<CMeta> cols = table.cols;
                List<MongoCMeta> mongoCols = table.getCols().stream().map((c) -> (MongoCMeta) c).collect(Collectors.toList());
                presentCols = MongoCMeta.getMongoPresentCols(mongoCols, true, zoneId);

//                List<MongoCMeta.MongoDocSplitCMeta> splitFieldMetas = null;
//                for (CMeta col : cols) {
//
//                    MongoCMeta mongoCol = (MongoCMeta) col;
//                    if (mongoCol.isMongoDocType()) {
//                        splitFieldMetas = mongoCol.getDocFieldSplitMetas();
//                        for (MongoCMeta.MongoDocSplitCMeta scol : splitFieldMetas) {
//                            presentCols.add(Pair.of(scol, (doc) -> {
//                                BsonValue nestVal = getEmbeddedValue(scol.getEmbeddedKeys(), doc);
//                                return MongoDataXColUtils.createCol(scol, nestVal);
//                            }));
//                        }
//                    }
//
//                    if (col.isDisable()) {
//                        continue;
//                    }
//
//                    presentCols.add(Pair.of(mongoCol, (doc) -> {
//                        BsonValue val = doc.get(mongoCol.getName());
//                        //  Object val = doc.get(mongoCol.getName(), Object.class);
//                        return MongoDataXColUtils.createCol(mongoCol, val);
//                    }));
//                }
                if (CollectionUtils.isEmpty(presentCols)) {
                    throw new IllegalStateException("presetnCols can not be empty");
                }
                presentCols = Collections.unmodifiableList(presentCols);
            }

            return presentCols;
        }

        @Override
        public Document getCollectionQueryFilter() {
            return tabExtend.filter.createFilter();
        }
    }

    private static BsonValue getEmbeddedValue(List<String> keys, BsonDocument doc) {
        BsonValue value = null;
        String key = null;
        Iterator<String> keyIterator = keys.iterator();

        while (keyIterator.hasNext()) {
            key = keyIterator.next();
            value = doc.get(key);
            if (value == null) {
                return null;
            }
            if (value.isDocument()) {
                doc = value.asDocument();
            } else {
                if (keyIterator.hasNext()) {
                    throw new IllegalStateException(String.format("At key %s, the value is not a BsonDocument (%s)", key, value.getClass().getName()));
                }
                return value;
            }
        }

        return doc;
    }

    /**
     * end implements: DBConfigGetter
     */
    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXMongodbReader.class, "DataXMongodbReader-tpl.json");
    }

    @Override
    protected boolean shallFillSelectedTabMeta() {
        // 不需要填充字段类型
        return false;
    }

    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        return new MongoDBReaderContext(jobName, tab, dumper, this);
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor //
            implements SubForm.ISubFormItemValidate, CompanionPluginFactory<SelectedTabExtend> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public SelectedTabExtend getCompanionPlugin(UploadPluginMeta pluginMeta) {
            String tabName = pluginMeta.getExtraParam(SubFormFilter.PLUGIN_META_SUBFORM_DETAIL_ID_VALUE);
            if (StringUtils.isEmpty(tabName)) {
                throw new IllegalArgumentException("param:" + SubFormFilter.PLUGIN_META_SUBFORM_DETAIL_ID_VALUE + " is not exsit in pluginMeta");
            }
            return SelectedTabExtend.getBatchPluginStore(pluginMeta.getPluginContext(),
                    pluginMeta.getDataXName(true)).find(tabName, false);
        }

        @Override
        public Descriptor<SelectedTabExtend> getCompanionDescriptor() {
            return TIS.get().getDescriptor(MongoSelectedTabExtend.class);
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

            return true;
        }

        @Override
        public boolean validateSubFormItems(IControlMsgHandler msgHandler, Context context,
                                            BaseSubFormProperties props, SubFormFilter subFormFilter,
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

                    final String fieldJsonPathKey = MongoCMetaCreatorFactory.splitMetasKey(colIndex, docSplitFieldIndex, MongoCMetaCreatorFactory.KEY_JSON_PATH);

                    jsonPath = splitCMeta.getJsonPath();

                    if (Validator.require.validate(msgHandler, context, fieldJsonPathKey, jsonPath)) {

                        Validator.validatePattern(msgHandler, context,
                                Validator.rule(MongoCMeta.MongoDocSplitCMeta.PATTERN_JSON_PATH, "需由带点字段名组成"),
                                fieldJsonPathKey, jsonPath);

                    }

                    name = splitCMeta.getName();
                    final String fieldNameKey = MongoCMetaCreatorFactory.splitMetasKey(colIndex, docSplitFieldIndex, nameKey);

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
                        msgHandler.addFieldError(context, MongoCMetaCreatorFactory.splitMetasKey(colIndex, docSplitFieldIndex, nameKey), "字段重复");

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

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.verify(msgHandler, context, postFormVals);
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

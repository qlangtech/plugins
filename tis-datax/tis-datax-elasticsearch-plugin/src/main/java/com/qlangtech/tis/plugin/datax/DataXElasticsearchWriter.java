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
import com.alibaba.datax.plugin.writer.elasticsearchwriter.DataConvertUtils;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESClient;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESColumn;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESFieldType;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.IInitialElasticSearchIndex;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISearchEngineTypeTransfer;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.elastic.ElasticEndpoint;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.VisualType;
import com.qlangtech.tis.solrdao.ISchema;
import com.qlangtech.tis.solrdao.ISchemaField;
import com.qlangtech.tis.solrdao.SchemaMetaContent;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
@Public
public class DataXElasticsearchWriter extends DataxWriter implements IDataxContext, ISearchEngineTypeTransfer, IInitialElasticSearchIndex {
    private static final String DATAX_NAME = "Elasticsearch";
    private static final String FIELD_ENDPOINT = "endpoint";
    public static VisualType ES_TYPE_TEXT
            = new VisualType(StringUtils.lowerCase(ESFieldType.TEXT.name()), true);


    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String index;

    @FormField(ordinal = 20, type = FormFieldType.ENUM, validate = {})
    public Boolean cleanup;

    @FormField(ordinal = 24, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer batchSize;

    @FormField(ordinal = 28, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer trySize;
    @FormField(ordinal = 32, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer timeout;

    @FormField(ordinal = 36, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean discovery;
    @FormField(ordinal = 40, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean compression;
    @FormField(ordinal = 44, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean multiThread;
    @FormField(ordinal = 48, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean ignoreWriteError;
    @FormField(ordinal = 52, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean ignoreParseError;

    @FormField(ordinal = 56, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name}, advance = true)
    public String alias;
    @FormField(ordinal = 60, type = FormFieldType.ENUM, validate = {}, advance = true)
    public String aliasMode;
    @FormField(ordinal = 64, type = FormFieldType.TEXTAREA, validate = {}, advance = true)
    public String settings;

    @FormField(ordinal = 68, type = FormFieldType.INPUTTEXT, validate = {}, advance = true)
    public String splitter;

    @FormField(ordinal = 75, type = FormFieldType.ENUM, validate = {}, advance = true)
    public Boolean dynamic;


    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    @Override
    public void startScanDependency() {
        getToken();
    }

    public ElasticEndpoint getToken() {
        ElasticEndpoint aliyunToken = ParamsConfig.getItem(endpoint, ElasticEndpoint.KEY_ELASTIC_SEARCH_DISPLAY_NAME);
        return aliyunToken;
    }

    @Override
    public boolean hasDifferWithSource(IPluginContext pluginCtx, ISelectedTab esTab, TableAlias tableAlias) {
        List<IColMetaGetter> cols = esTab.overwriteCols(pluginCtx, false);
//        IColMetaGetter col = null;
//        ISchemaField schemaCol = null;
        ISchema schema = convert2Schema(tableAlias);
        List<ISchemaField> schemaFields = schema.getSchemaFields();
        if (schemaFields.size() != cols.size()) {
            return true;
        }

        return false;
    }

    @Override
    public String getIndexName() {
        if (StringUtils.isEmpty(this.index)) {
            throw new IllegalArgumentException("prop index can not be empty");
        }
        return this.index;
    }

    @Override
    public SchemaMetaContent initSchemaMetaContent(IPluginContext pluginCtx, ISelectedTab tab) {

        SchemaMetaContent metaContent = new SchemaMetaContent();
        ESSchema schema = new ESSchema();
        metaContent.parseResult = schema;
        ESField field = null;

        List<IColMetaGetter> cols = tab.overwriteCols(pluginCtx, false);// RecordTransformerRules.overwriteCols(pluginCtx, tab);
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalStateException("table:" + tab.getName() + " relevant cols can not be empty");
        }
        for (IColMetaGetter m : cols) {
            field = convert(m);
            schema.fields.add(field);
        }
        byte[] schemaContent = null;
        metaContent.content = schemaContent;
        return metaContent;
    }

    private ESField convert(IColMetaGetter m) {
        ESField field = new ESField();
        field.setName(m.getName());
        field.setStored(true);
        if (m.isPk()) {
            field.setUniqueKey(true);
        }
        m.getType().accept(new CMetaTypeVisitor(field));
        field.setType(this.mapSearchEngineType(m.getType().getCollapse()));
        return field;
    }

    private static class CMetaTypeVisitor implements DataType.PartialTypeVisitor<Void> {
        private final ESField field;

        public CMetaTypeVisitor(ESField field) {
            this.field = field;
        }

        private Void typeVisit(DataType type) {
            field.setIndexed(true);
            return null;
        }

        @Override
        public Void bigInt(DataType type) {
            return typeVisit(type);
        }

        @Override
        public Void doubleType(DataType type) {
            return typeVisit(type);
        }

        @Override
        public Void dateType(DataType type) {
            return typeVisit(type);
        }

        @Override
        public Void timestampType(DataType type) {
            return typeVisit(type);
        }

        @Override
        public Void bitType(DataType type) {
            return typeVisit(type);
        }

        @Override
        public Void blobType(DataType type) {
            // return typeVisit(type);
            // 字节内容 需要index=false
            field.setIndexed(false);
            return null;
        }

        @Override
        public Void varcharType(DataType type) {
            return typeVisit(type);
        }
    }

    @Override
    public List<ESColumn> initialIndex(IDataxProcessor dataxProcessor) {
        ESTableAlias esSchema = null;
        Optional<TableAlias> first = dataxProcessor.getTabAlias(null).findFirst();
        if (first.isPresent()) {
            TableAlias value = first.get();
            if (!(value instanceof ESTableAlias)) {
                throw new IllegalStateException("value must be type of 'ESTableAlias',but now is :" + value.getClass());
            }
            esSchema = (ESTableAlias) value;
        }

        Objects.requireNonNull(esSchema, "esSchema can not be null");
        List<CMeta> cols = esSchema.getSourceCols();
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalStateException("cols can not be null");
        }
        Optional<CMeta> firstPK = cols.stream().filter((c) -> c.isPk()).findFirst();
        if (!firstPK.isPresent()) {
            throw new IllegalStateException("has not set PK col");
        }
        return this.initialIndex(esSchema);
    }

    /**
     * 当增量开始执行前，先需要初始化一下索引实例
     *
     * @param esSchema
     */
    public List<ESColumn> initialIndex(ESTableAlias esSchema) {
        if (esSchema == null) {
            throw new IllegalArgumentException("param esSchema can not be null");
        }
        ElasticEndpoint token = this.getToken();
        /********************************************************
         * 初始化索引Schema
         *******************************************************/
        AtomicReference<List<ESColumn>> colsRef = new AtomicReference<>();
        JSONArray schemaCols = ESTableAlias.getSchemaCols(esSchema.getSchemaContent());
        ESClient esClient = (token.createESClient());
        String type = null;
        try {
            final String esMapping = DataConvertUtils.genMappings(schemaCols, type, (columnList) -> {
                colsRef.set(columnList);
            });
            esClient.createIndex(this.getIndexName()
                    , type
                    , Pair.of(esMapping, Objects.requireNonNull(colsRef.get())), this.settings, false);
            return Objects.requireNonNull(colsRef.get(), "colsRef can not be null");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                esClient.closeJestClient();
            } catch (Throwable e) {
            }
        }
    }

    @Override
    public ISchema projectionFromExpertModel(IPluginContext context, ISelectedTab esTab, TableAlias tableAlias, Consumer<byte[]> schemaContentConsumer) {
        schemaContentConsumer.accept(((ESTableAlias) tableAlias).getSchemaByteContent());
        return convert2Schema(tableAlias);
    }

    private ISchema convert2Schema(TableAlias tableAlias) {
        ESTableAlias esTable = (ESTableAlias) tableAlias;

        JSONObject body = new JSONObject();
        body.put("content", esTable.getSchemaContent());

//        List<CMeta> cols = esTab.getCols();
//        AtomicInteger index = new AtomicInteger();
//        Map<String, CMetaProc> cmetaProc = cols.stream().filter((c) -> !c.isDisable()) //
//                .collect(Collectors.toMap((c) -> c.getName(), (c) -> new CMetaProc(index.getAndIncrement(), c)));

        // final Set<String> acceptKeys = //esTab.acceptedCols();// esTab.getCols().stream().filter((c) -> !c.isDisable()).map((c) -> c.getName()).collect(Collectors.toSet());
        ISchema schema = this.projectionFromExpertModel(body, (field) -> {
            return true;
//            CMetaProc proc = cmetaProc.get(field.getName());
//            if (proc != null) {
//                proc.setProcessed(true);
//                return true;
//            } else {
//                return false;
//            }
        });

//        cmetaProc.forEach((key, val) -> {
//            // 有某一列之前被删除了，后来又加回来了，需要重新加上
//            if (!val.processed) {
//                schema.getSchemaFields().add(val.index, convert(val.cm));
//            }
//        });
        return schema;
    }

//    private static class CMetaProc {
//        final CMeta cm;
//        final int index;
//
//        boolean processed;
//
//        public void setProcessed(boolean processed) {
//            this.processed = processed;
//        }
//
//        public CMetaProc(int index, CMeta cm) {
//            this.cm = cm;
//            this.index = index;
//        }
//    }

    @Override
    public ISchema projectionFromExpertModel(JSONObject body, Predicate<ISchemaField> fieldAcceptPredicate) {
        Objects.requireNonNull(body, "request body can not be null");
        final String content = body.getString("content");
        if (StringUtils.isBlank(content)) {
            throw new IllegalStateException("content can not be null");
        }
        ESSchema schema = new ESSchema();
        JSONObject field = null;
        ESField esField = null;
        JSONObject b = JSON.parseObject(content);

        JSONArray fields = b.getJSONArray(ESTableAlias.KEY_COLUMN);
        for (int i = 0; i < fields.size(); i++) {
            field = fields.getJSONObject(i);
            esField = new ESField();

            esField.setName(field.getString(ISchemaField.KEY_NAME));


            final String type = field.getString(ISchemaField.KEY_TYPE);
            VisualType visualType = parseVisualType(type);
            esField.setType(visualType);
            if (visualType.isSplit()) {
                esField.setTokenizerType(StringUtils.equalsIgnoreCase(EsTokenizerType.NULL.getKey(), type)
                        ? EsTokenizerType.NULL.getKey() : field.getString(ISchemaField.KEY_ANALYZER));
            }
            esField.setIndexed(field.getBooleanValue(ISchemaField.KEY_INDEX));
            esField.setMltiValued(field.getBooleanValue(ISchemaField.KEY_ARRAY));
            esField.setDocValue(field.getBooleanValue(ISchemaField.KEY_DOC_VALUES));
            esField.setStored(field.getBooleanValue(ISchemaField.KEY_STORE));
            esField.setUniqueKey(field.getBooleanValue(ISchemaField.KEY_PK));
            esField.setSharedKey(field.getBooleanValue(ISchemaField.KEY_SHARE_KEY));


            if (!fieldAcceptPredicate.test(esField)) {
                continue;
            }

            schema.fields.add(esField);
        }


        return schema;
    }

    /**
     * 小白模式转专家模式，正好与方法projectionFromExpertModel相反
     *
     * @param schema
     * @param expertSchema
     * @return
     */
    @Override
    public JSONObject mergeFromStupidModel(ISchema schema, JSONObject expertSchema) {
        JSONArray mergeTarget = expertSchema.getJSONArray(ESTableAlias.KEY_COLUMN);
        Objects.requireNonNull(mergeTarget, "mergeTarget can not be null");
        JSONObject f = null;
        Map<String, JSONObject> mergeFields = Maps.newHashMap();
        for (int i = 0; i < mergeTarget.size(); i++) {
            f = mergeTarget.getJSONObject(i);
            mergeFields.put(f.getString("name"), f);
        }

        JSONArray jFields = new com.alibaba.fastjson.JSONArray();

        for (ISchemaField field : schema.getSchemaFields()) {
            if (StringUtils.isBlank(field.getName())) {
                throw new IllegalStateException("field name can not be null");
            }
            f = mergeFields.get(field.getName());
            if (f == null) {
                f = new JSONObject();
                f.put(ISchemaField.KEY_NAME, field.getName());
            }

            VisualType type = EsTokenizerType.visualTypeMap.get(field.getTisFieldTypeName());
            if (type.isSplit()) {
                if (StringUtils.isEmpty(field.getTokenizerType())) {
                    throw new IllegalStateException("field:" + field.getName() + " relevant type is tokenizer but has not set analyzer");
                }
                if (StringUtils.endsWithIgnoreCase(field.getTokenizerType(), EsTokenizerType.NULL.getKey())) {
                    f.put(ISchemaField.KEY_TYPE, EsTokenizerType.NULL.getKey());
                    f.remove(ISchemaField.KEY_ANALYZER);
                } else {
                    f.put(ISchemaField.KEY_TYPE, type.getType());
                    f.put(ISchemaField.KEY_ANALYZER, field.getTokenizerType());
                }
            } else {
                f.put(ISchemaField.KEY_TYPE, type.getType());
                f.remove(ISchemaField.KEY_ANALYZER);
            }

            // TODO 还不确定array 是否对应multiValue的语义
            f.put(ISchemaField.KEY_ARRAY, field.isMultiValue());
            f.put(ISchemaField.KEY_DOC_VALUES, field.isDocValue());
            f.put(ISchemaField.KEY_INDEX, field.isIndexed());
            f.put(ISchemaField.KEY_STORE, field.isStored());
            if (field.isUniqueKey()) {
                f.put(ISchemaField.KEY_PK, true);
            }
            if (field.isSharedKey()) {
                f.put(ISchemaField.KEY_SHARE_KEY, true);
            }
            jFields.add(f);
        }

        expertSchema.put(ESTableAlias.KEY_COLUMN, jFields);
        return expertSchema;
    }


    private VisualType parseVisualType(String key) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("param key can not not be null");
        }
        if (StringUtils.equalsIgnoreCase(EsTokenizerType.NULL.getKey(), key)) {
            return ES_TYPE_TEXT;
        }
        for (Map.Entry<String, VisualType> entry : EsTokenizerType.visualTypeMap.entrySet()) {
            if (key.equals(entry.getKey())) {
                return entry.getValue();
            }
        }

        for (EsTokenizerType tType : EsTokenizerType.values()) {
            if (StringUtils.equals(tType.getKey(), key)) {
                return ES_TYPE_TEXT;
            }
        }

        return ES_TYPE_TEXT;
    }

    private static final Map<DataXReaderColType, VisualType> dataXTypeMapper;

    static {
        ImmutableMap.Builder<DataXReaderColType, VisualType> builder = ImmutableMap.builder();
        builder.put(DataXReaderColType.Long, createInitType(ESFieldType.LONG));
        builder.put(DataXReaderColType.INT, createInitType(ESFieldType.INTEGER));
        builder.put(DataXReaderColType.Double, createInitType(ESFieldType.DOUBLE));
        builder.put(DataXReaderColType.STRING, createInitType(ESFieldType.KEYWORD));
        builder.put(DataXReaderColType.Boolean, createInitType(ESFieldType.BOOLEAN));
        builder.put(DataXReaderColType.Date, createInitType(ESFieldType.DATE));
        builder.put(DataXReaderColType.Bytes, createInitType(ESFieldType.BINARY));
        dataXTypeMapper = builder.build();
    }


    private VisualType mapSearchEngineType(DataXReaderColType type) {

        VisualType esType = dataXTypeMapper.get(type);
        if (esType == null) {
            throw new IllegalStateException("illegal type:" + type);
        }

        return esType;
    }

    private static VisualType createInitType(ESFieldType esType) {
        return createInitType(esType, false);
    }

    private static VisualType createInitType(ESFieldType esType, boolean split) {
        return new VisualType(StringUtils.lowerCase(esType.name()), split);
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(
                DataXElasticsearchWriter.class, "DataXElasticsearchWriter-tpl.json");
    }

    @Override
    public String getTemplate() {
        return this.template;
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {

        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        IDataxProcessor.TableMap mapper = tableMap.get();
        if (!(mapper instanceof ESTableAlias)) {
            throw new IllegalStateException("mapper instance must be type of " + ESTableAlias.class.getSimpleName());
        }
        return new ESContext(this, (ESTableAlias) mapper);
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(FIELD_ENDPOINT, () -> ParamsConfig.getItems(ElasticEndpoint.KEY_ELASTIC_SEARCH_DISPLAY_NAME));
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.ElasticSearch;
        }

        public boolean validateSplitter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            String splitter = StringEscapeUtils.unescapeJava(value);
            if (StringUtils.length(splitter) != 1) {
                msgHandler.addFieldError(context, fieldName, "字符串长度必须为1");
                return false;
            }
            return true;
        }

        public boolean validateSettings(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                JSON.parseObject(value);
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "json解析有错误:" + e.getMessage());
                return false;
            }

            return true;
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSONArray fields = JSON.parseArray(value);
                JSONObject field = null;
                if (fields.size() < 1) {
                    msgHandler.addFieldError(context, fieldName, "请设置column");
                    return false;
                }

                String name = null;
                String type = null;
                StringBuffer err = new StringBuffer();
                for (int i = 0; i < fields.size(); i++) {
                    field = fields.getJSONObject(i);
                    name = field.getString("name");
                    type = field.getString("type");
                    if (StringUtils.isEmpty(name) || StringUtils.isEmpty(type)) {
                        err.append("第").append(i + 1).append("个name或者type为空,");
                    }
                }
                if (err.length() > 0) {
                    msgHandler.addFieldError(context, fieldName, err.toString());
                    return false;
                }
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "json解析有错误:" + e.getMessage());
                return false;
            }

            return true;
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
            return DATAX_NAME;
        }
    }
}

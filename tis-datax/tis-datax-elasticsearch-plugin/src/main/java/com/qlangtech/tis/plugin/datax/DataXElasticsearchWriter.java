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
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESClient;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESFieldType;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESInitialization;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISearchEngineTypeTransfer;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.VisualType;
import com.qlangtech.tis.solrdao.ISchema;
import com.qlangtech.tis.solrdao.ISchemaField;
import com.qlangtech.tis.solrdao.SchemaMetaContent;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXElasticsearchWriter extends DataxWriter implements IDataxContext, ISearchEngineTypeTransfer {
    private static final String DATAX_NAME = "Elasticsearch";
    private static final String FIELD_ENDPOINT = "endpoint";
    public static VisualType ES_TYPE_TEXT
            = new VisualType(StringUtils.lowerCase(ESFieldType.TEXT.name()), true);


    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String index;
    @FormField(ordinal = 16, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String type;

//    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String column;

    @FormField(ordinal = 20, type = FormFieldType.ENUM, validate = {})
    public Boolean cleanup;

    @FormField(ordinal = 24, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer batchSize;

    @FormField(ordinal = 28, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer trySize;
    @FormField(ordinal = 32, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer timeout;

    @FormField(ordinal = 36, type = FormFieldType.ENUM, validate = {})
    public Boolean discovery;
    @FormField(ordinal = 40, type = FormFieldType.ENUM, validate = {})
    public Boolean compression;
    @FormField(ordinal = 44, type = FormFieldType.ENUM, validate = {})
    public Boolean multiThread;
    @FormField(ordinal = 48, type = FormFieldType.ENUM, validate = {})
    public Boolean ignoreWriteError;
    @FormField(ordinal = 52, type = FormFieldType.ENUM, validate = {})
    public Boolean ignoreParseError;

    @FormField(ordinal = 56, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String alias;
    @FormField(ordinal = 60, type = FormFieldType.ENUM, validate = {})
    public String aliasMode;
    @FormField(ordinal = 64, type = FormFieldType.TEXTAREA, validate = {})
    public String settings;

    @FormField(ordinal = 68, type = FormFieldType.INPUTTEXT, validate = {})
    public String splitter;

    @FormField(ordinal = 75, type = FormFieldType.ENUM, validate = {})
    public Boolean dynamic;


    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public IAliyunToken getToken() {
        return IAliyunToken.getToken(this.endpoint);
    }

    public String getIndexName() {
        return this.index;
    }

    @Override
    public SchemaMetaContent initSchemaMetaContent(ISelectedTab tab) {

        SchemaMetaContent metaContent = new SchemaMetaContent();
        ESSchema schema = new ESSchema();
        metaContent.parseResult = schema;
        ESField field = null;
        for (ISelectedTab.ColMeta m : tab.getCols()) {
            field = new ESField();
            field.setName(m.getName());
            field.setStored(true);
            field.setIndexed(true);
            field.setType(this.mapSearchEngineType(m.getType()));
//            field.setSharedKey();
//            field.setUniqueKey();

            schema.fields.add(field);
        }
        byte[] schemaContent = null;
        metaContent.content = schemaContent;
        return metaContent;
    }

    /**
     * 当增量开始执行前，先需要初始化一下索引实例
     *
     * @param esSchema
     */
    public void initialIndex(ESTableAlias esSchema) {
        if (esSchema == null) {
            throw new IllegalArgumentException("param esSchema can not be null");
        }
        IAliyunToken token = this.getToken();
        /********************************************************
         * 初始化索引Schema
         *******************************************************/
        JSONArray schemaCols = esSchema.getSchemaCols();
        ESClient esClient = new ESClient(ESInitialization.create(token.getEndpoint(),
                token.getAccessKeyId(),
                token.getAccessKeySecret(),
                false,
                300000,
                false,
                false));
//        esClient.createClient(token.getEndpoint(),
//                token.getAccessKeyId(),
//                token.getAccessKeySecret(),
//                false,
//                300000,
//                false,
//                false);
        try {
            esClient.createIndex(this.getIndexName()
                    , this.type
                    , esClient.genMappings(schemaCols, this.type, (columnList) -> {
                    }), this.settings, false);
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
    public ISchema projectionFromExpertModel(IDataxProcessor.TableAlias tableAlias, Consumer<byte[]> schemaContentConsumer) {
        ESTableAlias esTable = (ESTableAlias) tableAlias;
        schemaContentConsumer.accept(esTable.getSchemaByteContent());
        JSONObject body = new JSONObject();
        body.put("content", esTable.getSchemaContent());
        return this.projectionFromExpertModel(body);
    }

    @Override
    public ISchema projectionFromExpertModel(JSONObject body) {
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

    private static final Map<ISelectedTab.DataXReaderColType, VisualType> dataXTypeMapper;

    static {
        ImmutableMap.Builder<ISelectedTab.DataXReaderColType, VisualType> builder = ImmutableMap.builder();
        builder.put(ISelectedTab.DataXReaderColType.Long, createInitType(ESFieldType.LONG));
        builder.put(ISelectedTab.DataXReaderColType.INT, createInitType(ESFieldType.INTEGER));
        builder.put(ISelectedTab.DataXReaderColType.Double, createInitType(ESFieldType.DOUBLE));
        builder.put(ISelectedTab.DataXReaderColType.STRING, createInitType(ESFieldType.KEYWORD));
        builder.put(ISelectedTab.DataXReaderColType.Boolean, createInitType(ESFieldType.BOOLEAN));
        builder.put(ISelectedTab.DataXReaderColType.Date, createInitType(ESFieldType.DATE));
        builder.put(ISelectedTab.DataXReaderColType.Bytes, createInitType(ESFieldType.BINARY));
        dataXTypeMapper = builder.build();
    }


    private VisualType mapSearchEngineType(ISelectedTab.DataXReaderColType type) {

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


    //    public static String getDftColumn() {
//        JSONArray cols = new JSONArray();
//        JSONObject col = null;
//        DataxReader reader = DataxWriter.dataReaderThreadlocal.get();
//        // Objects.requireNonNull(reader, "reader plugin can not be null");
//        if (reader == null) {
//            return "[]";
//        }
//        try {
//            List<ISelectedTab> selectedTabs = reader.getSelectedTabs();
//            Optional<ISelectedTab> first = selectedTabs.stream().findFirst();
//            if (!first.isPresent()) {
//                throw new IllegalStateException("can not find any selectedTabs");
//            }
//
//            for (ISelectedTab.ColMeta colMeta : first.get().getCols()) {
//                col = new JSONObject();
//                col.put("name", colMeta.getName());
//                col.put("type", colMeta.getType().getLiteria());
//                cols.add(col);
//            }
//
//            return JsonUtil.toString(cols);
//        } finally {
//            DataxWriter.dataReaderThreadlocal.remove();
//        }
//    }

    @Override
    public String getTemplate() {
        return this.template;
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {

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
            registerSelectOptions(FIELD_ENDPOINT, () -> ParamsConfig.getItems(IAliyunToken.class));
        }

        @Override
        protected EndType getEndType() {
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

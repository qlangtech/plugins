package com.qlangtech.tis.plugin.solr.schema;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Sets;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 支持json的FieldType
 *
 * @author: baisui 百岁
 * @create: 2021-01-26 12:36
 **/
public class JSONFieldTypeFactory extends FieldTypeFactory {
    private static final Logger logger = LoggerFactory.getLogger(JSONFieldTypeFactory.class);

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.identity})
    public String propPrefix;

    @FormField(ordinal = 2, validate = {})
    public String includeKeys;

    @Override
    public ISolrFieldType createInstance() {
        JSONField jsonField = new JSONField();
        jsonField.propPrefix = this.propPrefix;
        if (StringUtils.isNotEmpty(includeKeys)) {
            jsonField.includeKeys = Sets.newHashSet(StringUtils.split(includeKeys, ","));
        }
        logger.info("create json field,name:" + this.name + ",propPrefix:" + propPrefix + ",includeKeys:" + includeKeys);
        return jsonField;
    }


//    public static void main(String[] args) {
//        Matcher matcher = pattern_includeKeys.matcher("aaa,b_Bbm,ccc");
//        System.out.println(matcher.matches());
//    }

    private static class JSONField extends StrField implements ISolrFieldType {
        private String propPrefix;
        private Set<String> includeKeys;

        @Override
        protected void setArgs(IndexSchema schema, Map<String, String> args) {
            super.setArgs(schema, args);
        }

        @Override
        public List<IndexableField> createFields(SchemaField sf, Object value) {
            List<IndexableField> result = new ArrayList<>();
            String textValue = String.valueOf(value);
            if (value == null || !StringUtils.startsWith(textValue, "{")) {
                return Collections.emptyList();
            }
            JSONTokener tokener = new JSONTokener(textValue);
            JSONObject json = null;
            if (includeKeys != null && !this.includeKeys.isEmpty()) {
                json = new JSONObject(tokener, this.includeKeys.toArray(new String[]{}));
            } else {
                json = new JSONObject(tokener);
            }
            if ((sf.getProperties() & STORED) > 0) {
                result.add(this.createField(new SchemaField(sf.getName(), sf.getType()
                        , OMIT_NORMS | OMIT_TF_POSITIONS | STORED, ""), json.toString()));
            }
            if (StringUtils.isNotEmpty(this.propPrefix)) {
                SchemaField field = null;
                String fieldValue = null;
                for (String key : json.keySet()) {
                    field = new SchemaField(propPrefix + key, sf.getType(), OMIT_NORMS | OMIT_TF_POSITIONS | STORED | INDEXED, "");
                    fieldValue = json.getString(key);
                    if ("null".equalsIgnoreCase(fieldValue) || (includeKeys != null && !this.includeKeys.contains(key))) {
                        continue;
                    }
                    result.add(this.createField(field, fieldValue));
                }
            }
            return result;
        }

        @Override
        public boolean isPolyField() {
            return true;
        }
    }


    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<FieldTypeFactory> {
        static final Pattern pattern_includeKeys = Pattern.compile("(,?[a-zA-Z]{1}[\\da-zA-Z_\\-]+)+");

        @Override
        public String getDisplayName() {
            return "json";
        }

        public boolean validateIncludeKeys(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = pattern_includeKeys.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "不符合规范" + pattern_includeKeys);
                return false;
            }
            return true;
        }
    }
}

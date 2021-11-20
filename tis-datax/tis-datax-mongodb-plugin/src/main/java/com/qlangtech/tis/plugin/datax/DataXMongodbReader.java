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
import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * https://gitee.com/mirrors/DataX/blob/master/mongodbreader/doc/mongodbreader.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader
 **/
public class DataXMongodbReader extends DataxReader {

    public static final String DATAX_NAME = "MongoDB";
    public static final String TYPE_ARRAY = "array";
    public static final Set<String> acceptTypes = Sets.newHashSet("int", "long", "double", "string", TYPE_ARRAY, "date", "boolean", "bytes");

    private static final Logger logger = LoggerFactory.getLogger(DataXMongodbReader.class);

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String collectionName;
    @FormField(ordinal = 4, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;

    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public String query;

    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    /**
     * end implements: DBConfigGetter
     */
    public MangoDBDataSourceFactory getDsFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return (MangoDBDataSourceFactory) dsStore.getPlugin();
    }


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXMongodbReader.class, "DataXMongodbReader-tpl.json");
    }


    @Override
    public List<ISelectedTab> getSelectedTabs() {
        MongoDBTable tab = new MongoDBTable();

        List<ColCfg> cols = JSON.parseArray(this.column, ColCfg.class);
        tab.cols = cols.stream().map((c) -> {
            ISelectedTab.ColMeta colMeta = new ISelectedTab.ColMeta();
            colMeta.setName(c.getName());
            colMeta.setType(convertType(c.getType()));
            return colMeta;
        }).collect(Collectors.toList());

        return Collections.singletonList(tab);
    }

    private ColumnMetaData.DataType convertType(String type) {
        if (!acceptTypes.contains(type)) {
            throw new IllegalArgumentException("illegal type:" + type);
        }
        switch (type) {
            case "int":
            case "long":
                return ISelectedTab.DataXReaderColType.Long.dataType;
            case "double":
                return ISelectedTab.DataXReaderColType.Double.dataType;
            case "string":
            case "array":
                return ISelectedTab.DataXReaderColType.STRING.dataType;
            case "date":
                return ISelectedTab.DataXReaderColType.Date.dataType;
            case "boolean":
                return ISelectedTab.DataXReaderColType.Boolean.dataType;
            case "bytes":
                return ISelectedTab.DataXReaderColType.Bytes.dataType;
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

    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        IDataxReaderContext readerContext = new MongoDBReaderContext(this.collectionName, this);
        return Collections.singleton(readerContext).iterator();
    }

    @Override
    public String getTemplate() {
        return this.template;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateColumnContent(msgHandler, context, fieldName, value);
        }

        @Override
        protected EndType getEndType() {
            return EndType.MongoDB;
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

    public static boolean validateColumnContent(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
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
                        msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列name属性" + ValidatorCommons.MSG_DB_COL_NAME_ERROR);
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
                            msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个列'type'为'" + TYPE_ARRAY + "'的属性必须有'splitter'属性");
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

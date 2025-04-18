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
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.starrocks.StarRocksAutoCreateTable.BasicCreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.starrocks.StarRocksWriterContext;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.starrocks.StarRocksSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-29 14:46
 **/
public abstract class BasicStarRocksWriter extends BasicDataXRdbmsWriter<StarRocksSourceFactory> {

    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String loadProps;
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxBatchRows;

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        return new StarRocksWriterContext(this, tableMap.get(), transformerRules);
    }

    /**
     * 提增量处理模块使用
     *
     * @return
     */
    public abstract Separator getSeparator();


    public JSONObject getLoadProps() {
        return JSON.parseObject(loadProps);
    }


    public interface Separator {
        String COL_SEPARATOR = "column_separator";
        String ROW_DELIMITER = "row_delimiter";

        String COL_SEPARATOR_DEFAULT = "\\\\x01";
        String ROW_DELIMITER_DEFAULT = "\\\\x02";

        String getColumnSeparator();

        String getRowDelimiter();
    }


//    @Override
//    public final CreateTableSqlBuilder.CreateDDL generateCreateDDL(
//            SourceColMetaGetter sourceColMetaGetter, IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
////        if (!this.autoCreateTable) {
////            return null;
////        }
//        // https://doris.apache.org/docs/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE
//        // https://docs.starrocks.io/zh-cn/2.4/sql-reference/sql-statements/data-definition/CREATE%20TABLE
//        final BasicCreateTableSqlBuilder createTableSqlBuilder = createSQLDDLBuilder(sourceColMetaGetter, tableMapper, transformers);
//
//        return createTableSqlBuilder.build();
//    }

//    protected abstract BasicCreateTableSqlBuilder createSQLDDLBuilder(
//            SourceColMetaGetter sourceColMetaGetter, IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers);


    //    public static DataType.TypeVisitor<String> getDorisColumnTokenRecognise() {
//        return columnTokenRecognise;
//    }

    protected static abstract class BaseDescriptor extends RdbmsWriterDescriptor {
        public BaseDescriptor() {
            super();
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        protected int getMaxBatchSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }


        public boolean validateMaxBatchRows(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            int batchRows = Integer.parseInt(value);
            final int MaxBatchRows = 10000;
            if (batchRows < MaxBatchRows) {
                msgHandler.addFieldError(context, fieldName, "批次提交记录数不能小于:'" + MaxBatchRows + "'");
                return false;
            }
            return true;
        }

        public boolean validateLoadProps(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSONObject props = JSON.parseObject(value);
                boolean valid = true;
                if (StringUtils.isEmpty(props.getString(getColSeparatorKey()))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + getColSeparatorKey() + "'");
                    valid = false;
                }
                if (StringUtils.isEmpty(props.getString(getRowDelimiterKey()))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + getRowDelimiterKey() + "'");
                    valid = false;
                }

                return valid;
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "错误的JSON格式：" + e.getMessage());
                return false;
            }
        }


        protected abstract String getRowDelimiterKey();

        protected abstract String getColSeparatorKey();


//        @Override
//        public  abstract EndType getEndType();
//        {
//            return EndType.StarRocks;
//        }

//        @Override
//        public String getDisplayName() {
//            return DorisSourceFactory.NAME_DORIS;
//        }
    }
}

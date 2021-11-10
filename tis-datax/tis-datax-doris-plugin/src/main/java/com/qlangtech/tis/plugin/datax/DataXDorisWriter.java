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
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * reference: https://github.com/DorisDB/DataX/blob/master/doriswriter/doc/doriswriter.md
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:39
 * @see com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriter
 **/
public class DataXDorisWriter extends BasicDataXRdbmsWriter<DorisSourceFactory> {

    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {})
    public String loadProps;
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxBatchRows;

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        return new DorisWriterContext(this, tableMap.get());
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDorisWriter.class, "DataXDorisWriter-tpl.json");
    }

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }
        // https://doris.apache.org/master/zh-CN/sql-reference/sql-statements/Data%20Definition/CREATE%20TABLE.html#create-table

        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper) {
            @Override
            protected void appendExtraColDef(List<ISelectedTab.ColMeta> pks) {
//                if (pk != null) {
//                    script.append("  PRIMARY KEY (`").append(pk.getName()).append("`)").append("\n");
//                }
            }

            @Override
            protected void appendTabMeta(List<ISelectedTab.ColMeta> pks) {
                script.append(" ENGINE=olap").append("\n");
                script.append("DISTRIBUTED BY HASH(");
                if (pks.size() > 0) {
                    script.append(pks.stream().map((pk) -> pk.getName()).collect(Collectors.joining(",")));
                } else {
                    List<ISelectedTab.ColMeta> cols = this.getCols();
                    Optional<ISelectedTab.ColMeta> firstCol = cols.stream().findFirst();
                    if (firstCol.isPresent()) {
                        script.append(firstCol.get().getName());
                    } else {
                        throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
                    }
                }
                script.append(")\n");
                script.append("BUCKETS 10\n");
                script.append("PROPERTIES(\"replication_num\" = \"1\")");
                //script.append("DISTRIBUTED BY HASH(customerregister_id)");
            }

            protected String convertType(ISelectedTab.ColMeta col) {
                switch (col.getType()) {
                    case Long:
                        return "BIGINT";
                    case INT:
                        return "INT";
                    case Double:
                        return "DOUBLE";
                    case Date:
                        return "DATE";
                    case STRING:
                    case Boolean:
                    case Bytes:
                    default:
                        return "VARCHAR(150)";
                }
            }
        };
        return createTableSqlBuilder.build();
    }

    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        protected int getMaxBatchSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        public boolean validateLoadProps(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSON.parseObject(value);
                return true;
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
        }

        @Override
        protected EndType getEndType() {
            return EndType.Doris;
        }

        @Override
        public String getDisplayName() {
            return DorisSourceFactory.NAME_DORIS;
        }
    }
}

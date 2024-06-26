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

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.sqlserverwriter.SqlServerWriter
 **/
@Public
public class DataXSqlserverWriter extends BasicDataXRdbmsWriter<SqlServerDatasourceFactory> {

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXSqlserverWriter.class, "DataXSqlserverWriter-tpl.json");
    }

//    @Override
//    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
//        InitWriterTable.process(this.dataXName, targetTabName, jdbcUrls);
//    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        SqlServerWriterContext writerContext = new SqlServerWriterContext(this, tableMap.get(), transformerRules);
        return writerContext;
    }


    @Override
    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(
            IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
//        if (!this.autoCreateTable) {
//            return null;
//        }
        // https://www.cnblogs.com/mingfei200169/articles/427591.html
        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper, this.getDataSourceFactory(), transformers) {

            private String convertType(IColMetaGetter col) {
                //https://www.cnblogs.com/liberty777/p/10748570.html
                StringBuffer createSql = new StringBuffer(getSqlServerType(col));
                if (col.isPk()) {
                    createSql.append(" primary key ");
                }
                return createSql.toString();
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c) {
                    @Override
                    public String getMapperType() {
                        return convertType(this.meta);
                    }
                };
            }

            private String getSqlServerType(IColMetaGetter col) {
                DataType type = col.getType();
                switch (type.getJdbcType()) {
                    case INTEGER:
                    case TINYINT:
                    case SMALLINT:
                        return "int";
                    case BIGINT:
                        return "bigint";
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                        return "decimal(8,4)";
                    case DATE:
                    case TIME:
                    case TIMESTAMP:
                        return "datetime";
                    case BIT:
                    case BOOLEAN:
                        return "bit";
                    case BLOB:
                    case BINARY:
                    case LONGVARBINARY:
                    case VARBINARY:
                        //https://learn.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
                        // Variable-length binary data. n can be a value from 1 through 8,000.
                        // type.columnSize 可能为0 所以要用Math.max() 调整一下
                        return "varbinary(" + Math.min(Math.max(type.getColumnSize(), 300), 8000) + ")";
                    case LONGVARCHAR:
                        return "text";
                    default:
                        return "varchar(" + type.getColumnSize() + ")";
                }
            }


            @Override
            protected void appendExtraColDef(List<String> pk) {

            }

            @Override
            protected void appendTabMeta(List<String> pk) {

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
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.SqlServer;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DataXSqlserverReader.DATAX_NAME;
        }
    }
}

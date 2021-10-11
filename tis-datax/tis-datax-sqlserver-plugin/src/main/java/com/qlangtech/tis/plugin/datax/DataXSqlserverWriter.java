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

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.sqlserverwriter.SqlServerWriter
 **/
public class DataXSqlserverWriter extends BasicDataXRdbmsWriter<SqlServerDatasourceFactory> {

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXSqlserverWriter.class, "DataXSqlserverWriter-tpl.json");
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        SqlServerWriterContext writerContext = new SqlServerWriterContext(this, tableMap.get());
        return writerContext;
    }


    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }
        // https://www.cnblogs.com/mingfei200169/articles/427591.html
        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper) {
            @Override
            protected String convertType(ISelectedTab.ColMeta col) {
                //https://www.cnblogs.com/liberty777/p/10748570.html
                StringBuffer createSql = new StringBuffer(getSqlServerType(col));
                if (col.isPk()) {
                    createSql.append(" primary key ");
                }
                return createSql.toString();
            }

            private String getSqlServerType(ISelectedTab.ColMeta col) {
                switch (col.getType()) {
                    case Long:
                        return "bigint";
                    case INT:
                        return "int";
                    case Double:
                        return "decimal(8,4)";
                    case Date:
                        return "datetime";
                    case STRING:
                    case Boolean:
                    case Bytes:
                    default:
                        return "varchar(100)";
                }
            }

            @Override
            protected char colEscapeChar() {
                return '\"';
            }

            @Override
            protected void appendExtraColDef(List<ISelectedTab.ColMeta> pk) {

            }

            @Override
            protected void appendTabMeta(List<ISelectedTab.ColMeta> pk) {

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
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DataXSqlserverReader.DATAX_NAME;
        }
    }
}

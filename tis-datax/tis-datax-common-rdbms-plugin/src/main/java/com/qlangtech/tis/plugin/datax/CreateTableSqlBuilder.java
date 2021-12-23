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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-08-07 13:45
 **/
public abstract class CreateTableSqlBuilder {
    private final IDataxProcessor.TableMap tableMapper;
    protected StringBuffer script;

    public CreateTableSqlBuilder(IDataxProcessor.TableMap tableMapper) {
        this.tableMapper = tableMapper;
        this.script = new StringBuffer();
    }


    protected void appendExtraColDef(List<ColWrapper> pks) {
    }

    protected void appendTabMeta(List<ColWrapper> pks) {
    }

    public StringBuffer build() {

        script.append("CREATE TABLE ").append(getCreateTableName()).append("\n");
        script.append("(\n");
        List<ColWrapper> pks = Lists.newArrayList();
        int maxColNameLength = 0;
        for (ISelectedTab.ColMeta col : this.getCols()) {
            if (col.isPk()) {
                pks.add(this.createColWrapper(col));
            }
            int m = StringUtils.length(col.getName());
            if (m > maxColNameLength) {
                maxColNameLength = m;
            }
        }
        maxColNameLength += 4;
        final int colSize = getCols().size();
        int colIndex = 0;
        String escapeChar = supportColEscapeChar() ? String.valueOf(colEscapeChar()) : StringUtils.EMPTY;
        for (ColWrapper col : preProcessCols(pks, getCols())) {
            script.append("    ").append(escapeChar)
                    .append(String.format("%-" + (maxColNameLength) + "s", col.getName() + (escapeChar)))
                    .append(col.getMapperType());
            col.appendExtraConstraint(script);
            if (++colIndex < colSize) {
                script.append(",");
            }
            script.append("\n");
        }

        // script.append("    `__cc_ck_sign` Int8 DEFAULT 1").append("\n");
        this.appendExtraColDef(pks);
        script.append(")\n");
        this.appendTabMeta(pks);
//            script.append(" ENGINE = CollapsingMergeTree(__cc_ck_sign)").append("\n");
//            // Objects.requireNonNull(pk, "pk can not be null");
//            if (pk != null) {
//                script.append(" ORDER BY `").append(pk.getName()).append("`\n");
//            }
//            script.append(" SETTINGS index_granularity = 8192");
//        CREATE TABLE tis.customer_order_relation
//                (
//                        `customerregister_id` String,
//                        `waitingorder_id` String,
//                        `worker_id` String,
//                        `kind` Int8,
//                        `create_time` Int64,
//                        `last_ver` Int8,
//                        `__cc_ck_sign` Int8 DEFAULT 1
//                )
//        ENGINE = CollapsingMergeTree(__cc_ck_sign)
//        ORDER BY customerregister_id
//        SETTINGS index_granularity = 8192
        return script;
    }


    /**
     * 在打印之前先对cols进行预处理，比如排序等
     *
     * @param cols
     * @return
     */
    protected List<ColWrapper> preProcessCols(List<ColWrapper> pks, List<ISelectedTab.ColMeta> cols) {
        return cols.stream().map((c) -> createColWrapper(c)).collect(Collectors.toList());
    }

    protected abstract ColWrapper createColWrapper(ISelectedTab.ColMeta c);//{
//        return new ColWrapper(c);
    // }

    public static abstract class ColWrapper {
        protected final ISelectedTab.ColMeta meta;

        public ColWrapper(ISelectedTab.ColMeta meta) {
            this.meta = meta;
        }

        public abstract String getMapperType();

        protected void appendExtraConstraint(StringBuffer ddlScript) {

        }

        public String getName() {
            return this.meta.getName();
        }
    }


    protected char colEscapeChar() {
        return '`';
    }

    protected boolean supportColEscapeChar() {
        return true;
    }

    protected String getCreateTableName() {
        return tableMapper.getTo();
    }

    protected List<ISelectedTab.ColMeta> getCols() {
        return tableMapper.getSourceCols();
    }
}

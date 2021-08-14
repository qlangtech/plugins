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

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import org.apache.commons.lang.StringUtils;

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

    protected abstract String convertType(ISelectedTab.ColMeta col);

    protected void appendExtraColDef(ISelectedTab.ColMeta pk) {
    }

    protected void appendTabMeta(ISelectedTab.ColMeta pk) {
    }

    public StringBuffer build() {

        script.append("CREATE TABLE ").append(tableMapper.getTo()).append("\n");
        script.append("(\n");
        ISelectedTab.ColMeta pk = null;
        int maxColNameLength = 0;
        for (ISelectedTab.ColMeta col : tableMapper.getSourceCols()) {
            int m = StringUtils.length(col.getName());
            if (m > maxColNameLength) {
                maxColNameLength = m;
            }
        }
        maxColNameLength += 4;
        for (ISelectedTab.ColMeta col : tableMapper.getSourceCols()) {
            if (pk == null && col.isPk()) {
                pk = col;
            }
            script.append("    `").append(String.format("%-" + (maxColNameLength) + "s", col.getName() + "`"))
                    .append(convertType(col)).append(",").append("\n");
        }
        // script.append("    `__cc_ck_sign` Int8 DEFAULT 1").append("\n");
        this.appendExtraColDef(pk);
        script.append(")\n");
        this.appendTabMeta(pk);
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
}

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

import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-08 14:35
 **/
public enum MREngine {
    SPARK(2), HIVE(1);

    private final int columnIndexGetTableName;

    public List<String> getTabs(Connection connection, EntityName dumpTable) throws Exception {
        final List<String> tables = new ArrayList<>();
        // SPARK:
        //        +-----------+---------------+--------------+--+
        //        | database  |   tableName   | isTemporary  |
        //        +-----------+---------------+--------------+--+
        //        | order     | totalpayinfo  | false        |
        //        +-----------+---------------+--------------+--+
        // Hive
        HiveDBUtils.query(connection, "show tables in " + dumpTable.getDbName()
                , result -> tables.add(result.getString(this.columnIndexGetTableName)));
        return tables;
    }

    private MREngine(int columnIndexGetTableName) {
        this.columnIndexGetTableName = columnIndexGetTableName;
    }
}

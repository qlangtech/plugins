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

package com.qlangtech.tis.plugin.ds.cassandra;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-21 14:29
 **/
public class CassandraDumper implements IDataSourceDumper {

    public final TISTable table;

    public CassandraDumper(TISTable table) {
        this.table = table;
    }

    @Override
    public void closeResource() {

    }

    @Override
    public int getRowSize() {
        return 0;
    }

    @Override
    public List<ColumnMetaData> getMetaData() {
        return null;
    }

    @Override
    public Iterator<Map<String, String>> startDump() {
        return null;
    }

    @Override
    public String getDbHost() {
        return null;
    }
}

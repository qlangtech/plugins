/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugins.incr.flink.connector.clickhouse;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;

import java.io.Serializable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-02 14:11
 **/
public class ColMeta implements Serializable {
    private String key;
    private ColumnMetaData.DataType type;

    public ColMeta(String key, ColumnMetaData.DataType type) {
        this.key = key;
        this.type = type;
    }

    public ColMeta() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ColumnMetaData.DataType getType() {
        return type;
    }

    public void setType(ColumnMetaData.DataType type) {
        this.type = type;
    }
}

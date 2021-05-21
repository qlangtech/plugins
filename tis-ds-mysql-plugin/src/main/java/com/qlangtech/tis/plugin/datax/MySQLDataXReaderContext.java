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

import com.qlangtech.tis.datax.IDataxReaderContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author: baisui 百岁
 * @create: 2021-04-20 17:42
 **/
public class MySQLDataXReaderContext extends MySQLDataxContext implements IDataxReaderContext {
    private final String name;
    private final String sourceTableName;
    private String where;

    public boolean isContainWhere() {
        return StringUtils.isNotBlank(this.where);
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    @Override
    public String getTaskName() {
        return this.name;
    }

    @Override
    public String getSourceEntityName() {
        return this.sourceTableName;
    }

    public MySQLDataXReaderContext(String name, String sourceTableName) {
        this.name = name;
        this.sourceTableName = sourceTableName;
    }


}

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

import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author: baisui 百岁
 * @create: 2021-04-20 17:42
 **/
public class MySQLDataXReaderContext extends RdbmsReaderContext {
    private final RdbmsDataxContext rdbmsContext;

    public MySQLDataXReaderContext(String name, String sourceTableName, RdbmsDataxContext mysqlContext) {
        super(name, sourceTableName, null, null);
        this.rdbmsContext = mysqlContext;
    }

    public String getDataXName() {
        return rdbmsContext.getDataXName();
    }

    public String getTabName() {
        return rdbmsContext.getTabName();
    }

    public String getPassword() {
        return rdbmsContext.getPassword();
    }

    public String getUsername() {
        return rdbmsContext.getUsername();
    }

    public String getJdbcUrl() {
        if (StringUtils.isEmpty(rdbmsContext.getJdbcUrl())) {
            throw new NullPointerException("rdbmsContext.getJdbcUrl() can not be empty");
        }
        return rdbmsContext.getJdbcUrl();
    }


//    public boolean isContainWhere() {
//        return StringUtils.isNotBlank(this.where);
//    }
//
//    public String getWhere() {
//        return where;
//    }
//
//    public void setWhere(String where) {
//        this.where = where;
//    }


}

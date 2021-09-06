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
import org.apache.commons.lang.StringUtils;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 11:06
 **/
public class RdbmsDataxContext {
    private final String dataXName;
    public IDataxProcessor.TabCols cols;
    String tabName;
    String password;
    String username;
    String jdbcUrl;

    public RdbmsDataxContext(String dataXName) {
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("param dataXName:" + dataXName + " can not be null");
        }
        this.dataXName = dataXName;
    }

    public String getDataXName() {
        return this.dataXName;
    }

    public String getColsQuotes() {
        return cols.getColsQuotes();
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getTabName() {
        return this.tabName;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }
}

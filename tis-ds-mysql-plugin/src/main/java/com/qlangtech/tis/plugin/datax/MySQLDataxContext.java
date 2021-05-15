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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 11:06
 **/
public class MySQLDataxContext implements IDataxContext {
    String tabName;
    String password;
    String username;
    String jdbcUrl;
    List<String> cols = new ArrayList<>();


    public String getTabName() {
        return tabName;
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

    public String getColsQuotes() {
        return getColumnWithLink("\"`", "`\"");
    }

    public String getCols() {
        return getColumnWithLink("`", "`");
    }

    private String getColumnWithLink(String left, String right) {
        return this.cols.stream().map(r -> (left + r + right)).collect(Collectors.joining(","));
    }
}

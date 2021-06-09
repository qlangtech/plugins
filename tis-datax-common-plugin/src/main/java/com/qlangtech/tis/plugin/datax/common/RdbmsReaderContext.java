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

package com.qlangtech.tis.plugin.datax.common;

import com.qlangtech.tis.datax.IDataxReaderContext;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 10:22
 **/
public class RdbmsReaderContext implements IDataxReaderContext {
    private final String name;
    private final String sourceTableName;
    private String where;
    private List<String> cols = new ArrayList<>();

    public RdbmsReaderContext(String jobName, String sourceTableName) {
        this.name = jobName;
        this.sourceTableName = sourceTableName;
    }

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

    public void setCols(List<String> cols) {
        this.cols = cols;
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

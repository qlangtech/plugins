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
import com.qlangtech.tis.datax.IDataxProcessor;
import org.apache.commons.lang3.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-09 16:08
 **/
public class ClickHouseWriterContext implements IDataxContext {
    private IDataxProcessor.TabCols cols;
    private String password;
    private String username;
    private String table;
    private String jdbcUrl;
    private Integer batchSize;
    private Integer batchByteSize;
    private String writeMode;

    private String dataXName;

    private String preSql;
    private String postSql;

    public String getDataXName() {
        return dataXName;
    }

    public void setDataXName(String dataXName) {
        this.dataXName = dataXName;
    }

    public boolean isContainPreSql() {
        return StringUtils.isNotEmpty(this.preSql);
    }

    public boolean isContainPostSql() {
        return StringUtils.isNotEmpty(this.postSql);
    }

    public String getPreSql() {
        return preSql;
    }

    public void setPreSql(String preSql) {
        this.preSql = preSql;
    }

    public String getPostSql() {
        return postSql;
    }

    public void setPostSql(String postSql) {
        this.postSql = postSql;
    }

    public IDataxProcessor.TabCols getCols() {
        return cols;
    }

    public void setCols(IDataxProcessor.TabCols cols) {
        this.cols = cols;
    }

    public boolean isContainPassword() {
        return StringUtils.isNotEmpty(this.password);
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public boolean isContainBatchSize() {
        return this.batchSize != null;
    }

    public boolean isContainBatchByteSize() {
        return this.batchByteSize != null;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getBatchByteSize() {
        return batchByteSize;
    }

    public void setBatchByteSize(Integer batchByteSize) {
        this.batchByteSize = batchByteSize;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public boolean isContainWriteMode() {
        return StringUtils.isNotEmpty(this.writeMode);
    }
}

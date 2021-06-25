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

package com.qlangtech.tis.plugin.datax.common;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-23 12:44
 **/
public abstract class RdbmsWriterContext<WRITER extends BasicDataXRdbmsWriter, DS extends BasicDataSourceFactory>
        extends BasicRdbmsContext<WRITER, DS> implements IDataxContext {
    private final String tableName;

    public RdbmsWriterContext(WRITER writer, IDataxProcessor.TableMap tabMapper) {
        super(writer, (DS) writer.getDataSourceFactory());
        this.tableName = tabMapper.getTo();
        this.setCols(tabMapper.getSourceCols().stream().map((c) -> c.getName()).collect(Collectors.toList()));
    }

    public String getTableName() {
        return this.tableName;
    }

    public boolean isContainPreSql() {
        return StringUtils.isNotBlank(this.plugin.preSql);
    }

    public boolean isContainPostSql() {
        return StringUtils.isNotBlank(this.plugin.postSql);
    }

    public String getPreSql() {
        return this.plugin.preSql;
    }

    public String getPostSql() {
        return this.plugin.postSql;
    }

    public boolean isContainSession() {
        return StringUtils.isNotEmpty(this.plugin.session);
    }

    public final String getSession() {
        return this.plugin.session;
    }

    public boolean isContainBatchSize() {
        return this.plugin.batchSize != null;
    }

    public int getBatchSize() {
        return this.plugin.batchSize;
    }


    public String getUserName() {
        return this.dsFactory.getUserName();
    }

    public String getPassword() {
        return this.dsFactory.getPassword();
    }

    public String getJdbcUrl() {
        List<String> jdbcUrls = this.dsFactory.getJdbcUrls();
        Optional<String> firstURL = jdbcUrls.stream().findFirst();
        if (!firstURL.isPresent()) {
            throw new IllegalStateException("can not find jdbc url");
        }
        return firstURL.get();
    }


}

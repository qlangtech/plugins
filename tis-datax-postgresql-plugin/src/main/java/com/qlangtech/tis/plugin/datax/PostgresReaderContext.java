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
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 13:15
 **/
public class PostgresReaderContext extends RdbmsReaderContext<DataXPostgresqlReader, PGDataSourceFactory> {

    public static final String colEscapeChar = "\\\"";

    public PostgresReaderContext(String jobName, String sourceTableName, IDataSourceDumper dumper, DataXPostgresqlReader reader) {
        super(jobName, sourceTableName, dumper, reader);
    }

    public String getUserName() {
        return dsFactory.getUserName();
    }

    public String getPassword() {
        return dsFactory.getPassword();
    }

    public boolean isContainSplitPk() {
        return this.plugin.splitPk != null;
    }

    public boolean isSplitPk() {
        return this.plugin.splitPk;
    }

    public boolean isContainFetchSize() {
        return this.plugin.fetchSize != null;
    }

    public int getFetchSize() {
        return this.plugin.fetchSize;
    }

    protected String colEscapeChar() {
        return colEscapeChar;
    }
}

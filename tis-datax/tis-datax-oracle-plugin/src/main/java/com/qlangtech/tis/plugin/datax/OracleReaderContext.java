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
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 15:41
 **/
public class OracleReaderContext extends RdbmsReaderContext<DataXOracleReader, OracleDataSourceFactory> {
    public OracleReaderContext(String jobName, String sourceTableName, IDataSourceDumper dumper, DataXOracleReader reader) {
        super(jobName, sourceTableName, dumper, reader);
    }

    public String getUserName() {
        return this.dsFactory.getUserName();
    }

    public boolean isContainPassword() {
        return StringUtils.isNotEmpty(this.dsFactory.getPassword());
    }

    public String getPassword() {
        return this.dsFactory.getPassword();
    }

//    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
//    public Boolean splitPk;
//
//    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {})
//    public Integer fetchSize;
//
//    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {})
//    public String session;

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

    public boolean isContainSession() {
        return StringUtils.isNotEmpty(this.plugin.session);
    }

    public String getSession() {
        return this.plugin.session;
    }

    @Override
    protected String colEscapeChar() {
        return StringUtils.EMPTY;
    }
}

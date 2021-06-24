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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.sqlserverreader.SqlServerReader
 **/
public class DataXSqlserverReader extends BasicDataXRdbmsReader<SqlServerDatasourceFactory> {
    public static final String DATAX_NAME = "SqlServer";

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
    public Boolean splitPk;

    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer fetchSize;


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXSqlserverReader.class, "DataXSqlserverReader-tpl.json");
    }

    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        SqlServerReaderContext readerContext = new SqlServerReaderContext(jobName, tab.getName(), dumper, this);
        return readerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

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
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;

/**
 * https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataxMySQLReader extends BasicDataXRdbmsReader<MySQLDataSourceFactory> {
    private static final String DATAX_NAME = "MySQL";

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require, Validator.identity})
    public boolean splitPk;


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxMySQLReader.class, "mysql-reader-tpl.json");
    }

    protected RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        MySQLDataSourceFactory dsFactory = this.getDataSourceFactory();
        RdbmsDataxContext rdbms = new RdbmsDataxContext();
        rdbms.setJdbcUrl(dumper.getDbHost());
        rdbms.setUsername(dsFactory.getUserName());
        rdbms.setPassword(dsFactory.getPassword());
        return new MySQLDataXReaderContext(jobName, tab.getName(), rdbms);
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

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
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.postgresqlreader.PostgresqlReader
 **/
public class DataXPostgresqlReader extends BasicDataXRdbmsReader<PGDataSourceFactory> {
    public static final String PG_NAME = "PostgreSQL";

//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;
    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean splitPk;
    //    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
//    public String where;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
//    public String querySql;
    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer fetchSize;


    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        PostgresReaderContext readerContext = new PostgresReaderContext(jobName, tab.getName(), dumper, this);
        return readerContext;
    }

    //    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXPostgresqlReader.class, "DataXPostgresqlReader-tpl.json");
    }


    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return PG_NAME;
        }
    }
}

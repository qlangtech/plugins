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

import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.cassandrareader.CassandraReader
 **/
public class DataXCassandraReader extends BasicDataXRdbmsReader<CassandraDatasourceFactory> {
    public static final String DATAX_NAME = "Cassandra";

    //    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;
//    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
//    public String where;
    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {})
    public Boolean allowFiltering;
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public String consistancyLevel;

    protected boolean isFilterUnexistCol() {
        return false;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXCassandraReader.class, "DataXCassandraReader-tpl.json");
    }

    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab
            , IDataSourceDumper dumper) {
        CassandraReaderContext readerContext = new CassandraReaderContext(jobName, tab, dumper ,this);
        return readerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends DataxReader.BaseDataxReaderDescriptor {
        @Override
        public boolean isRdbms() {
            return true;
        }

        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

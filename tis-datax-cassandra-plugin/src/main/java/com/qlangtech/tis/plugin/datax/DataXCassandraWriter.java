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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;

import java.util.Optional;

/**
 * @see com.alibaba.datax.plugin.writer.cassandrawriter.CassandraWriter
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXCassandraWriter extends DataxWriter {
    //private static final String DATAX_NAME = "Cassandra";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    //    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String host;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String port;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
//    public String username;
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
//    public String password;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {})
//    public String useSSL;
    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer connectionsPerHost;
    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxPendingPerConnection;
    //    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String keyspace;
//    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public String consistancyLevel;
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer batchSize;

    @FormField(ordinal = 12, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXCassandraWriter.class, "DataXCassandraWriter-tpl.json");
    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        return new CassandraWriterContext(this, tableMap.get());
    }

    public  CassandraDatasourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return (CassandraDatasourceFactory) dsStore.getPlugin();
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DataXCassandraReader.DATAX_NAME;
        }
    }
}

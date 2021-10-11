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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.postgresqlwriter.PostgresqlWriter
 **/
public class DataXPostgresqlWriter extends BasicDataXRdbmsWriter<PGDataSourceFactory> {


//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String jdbcUrl;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String username;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String password;
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;
//    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
//    public String preSql;
//    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
//    public String postSql;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
//    public String batchSize;
//
//    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXPostgresqlWriter.class, "DataXPostgresqlWriter-tpl.json");
    }

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }
        StringBuffer createDDL = new StringBuffer();

        return createDDL;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        PostgreWriterContext writerContext = new PostgreWriterContext(this, tableMap.get());

        return writerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return DataXPostgresqlReader.PG_NAME;
        }
    }
}

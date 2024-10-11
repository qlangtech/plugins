/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.postgresqlwriter.PostgresqlWriter
 **/
@Public
public class DataXPostgresqlWriter extends BasicDataXRdbmsWriter<PGDataSourceFactory> {

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXPostgresqlWriter.class, "DataXPostgresqlWriter-tpl.json");
    }

    public static void main(String[] args) {
        System.out.println(StringUtils.EMPTY.toCharArray()[0]);
    }

    @Override
    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        PGDataSourceFactory ds = this.getDataSourceFactory();

        final CreateTableSqlBuilder createTableSqlBuilder = new PostgreSQLCreateTableSqlBuilder(tableMapper, ds, transformers);
        return createTableSqlBuilder.build();
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        PostgreWriterContext writerContext = new PostgreWriterContext(this, tableMap.get(), transformerRules);

        return writerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Postgres;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DataXPostgresqlReader.PG_NAME;
        }
    }
}

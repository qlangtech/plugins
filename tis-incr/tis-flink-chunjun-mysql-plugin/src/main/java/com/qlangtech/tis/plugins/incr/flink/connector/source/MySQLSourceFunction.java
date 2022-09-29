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

package com.qlangtech.tis.plugins.incr.flink.connector.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.mysql.source.MysqlSourceFactory;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.qlangtech.tis.plugins.incr.flink.chunjun.source.ChunjunSourceFunction;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-28 14:34
 **/
public class MySQLSourceFunction extends ChunjunSourceFunction {
    public MySQLSourceFunction(MySQLSourceFactory sourceFactory) {
        super(sourceFactory);
    }

    @Override
    protected JdbcSourceFactory createChunjunSourceFactory(
            SyncConf conf, BasicDataSourceFactory sourceFactory, AtomicReference<SourceFunction<RowData>> sourceFunc) {
        return new ExtendMySQLSourceFactory(conf, sourceFactory) {
            protected DataStream<RowData> createInput(
                    InputFormat<RowData, InputSplit> inputFormat, String sourceName) {
                Preconditions.checkNotNull(sourceName);
                Preconditions.checkNotNull(inputFormat);
                DtInputFormatSourceFunction<RowData> function =
                        new DtInputFormatSourceFunction<>(inputFormat, getTypeInformation());
                sourceFunc.set(function);
                return null;
            }
        };
    }

    private static class ExtendMySQLSourceFactory extends MysqlSourceFactory {
        private final DataSourceFactory dataSourceFactory;

        public ExtendMySQLSourceFactory(SyncConf syncConf, DataSourceFactory dataSourceFactory) {
            super(syncConf, null);
            this.fieldList = syncConf.getReader().getFieldList();
            this.dataSourceFactory = dataSourceFactory;
        }

        @Override
        protected JdbcInputFormatBuilder getBuilder() {
            return new JdbcInputFormatBuilder(new TISMysqlInputFormat(dataSourceFactory));
        }
    }
}

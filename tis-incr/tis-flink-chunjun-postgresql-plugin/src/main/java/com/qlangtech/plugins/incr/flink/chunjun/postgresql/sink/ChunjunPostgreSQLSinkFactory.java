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

package com.qlangtech.plugins.incr.flink.chunjun.postgresql.sink;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.chunjun.postgresql.dialect.TISPostgresqlDialect;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.ColMetaUtils;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-14 16:45
 **/
public class ChunjunPostgreSQLSinkFactory extends ChunjunSinkFactory {

//    @Override
//    protected JdbcDialect createJdbcDialect(SyncConf syncConf) {
//        return new TISPostgresqlDialect(syncConf);
//    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        return TISPostgresqlDialect.class;
    }

    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(TableAlias tableAlias, DataSourceFactory dsFactory, JdbcConf conf) {
        return new TISPostgresOutputFormat(dsFactory, ColMetaUtils.getColMetasMap(this, tableAlias));
    }

    @Override
    protected void setSchema(Map<String, Object> conn, String dbName, BasicDataSourceFactory dsFactory) {
        if (!(dsFactory instanceof BasicDataSourceFactory.ISchemaSupported)) {
            throw new IllegalStateException("dsFactory:" + dsFactory.name
                    + " must be type of " + BasicDataSourceFactory.ISchemaSupported.class);
        }
        BasicDataSourceFactory.ISchemaSupported schemaSupported = (BasicDataSourceFactory.ISchemaSupported) dsFactory;
        super.setSchema(conn, schemaSupported.getDBSchema(), dsFactory);
    }

//    /**
//     * https://dtstack.github.io/chunjun/documents/ChunJun%E8%BF%9E%E6%8E%A5%E5%99%A8@postgresql@postgres-sink
//     *
//     * @param cm
//     * @return
//     */
//    @Override
//    protected String parseType(CMeta cm) {
//        return PostgreSQLSourceFunction.typeMapper(cm);
//    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(ChunjunPostgreSQLSinkFactory.class));
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {

    }

    @TISExtension
    public static class DftDesc extends BasicChunjunSinkDescriptor {
        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return IEndTypeGetter.EndType.Postgres;
        }
    }
}

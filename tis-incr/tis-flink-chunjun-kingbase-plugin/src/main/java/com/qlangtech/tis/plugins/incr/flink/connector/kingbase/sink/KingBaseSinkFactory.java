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

package com.qlangtech.tis.plugins.incr.flink.connector.kingbase.sink;

import com.alibaba.citrus.turbine.Context;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.google.common.collect.Sets;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.datax.kingbase.DataXKingBaseWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.kingbase.BasicKingBaseDataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.ColMetaUtils;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-25 09:52
 **/
public class KingBaseSinkFactory extends ChunjunSinkFactory {
    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        DataXKingBaseWriter kingBaseWriter = (DataXKingBaseWriter) DataxWriter.load(null, this.getCollectionName());
        BasicKingBaseDataSourceFactory dsFactory = kingBaseWriter.getDataSourceFactory();
        Class<JdbcDialect> jdbcDialectClass = (Class<JdbcDialect>) dsFactory.dbMode.getJdbcDialectClass();
        return Objects.requireNonNull(jdbcDialectClass, "jdbcDialectClass can not be null");
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(TableAlias tableAlias, DataSourceFactory dsFactory, JdbcConf jdbcConf) {
        KingBaseOutputFormat outputFormat
                = new KingBaseOutputFormat(dsFactory, ColMetaUtils.getColMetasMap(this, tableAlias));
        return outputFormat;
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

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {
        JdbcUtil.putExtParam(jdbcConf);
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(KingBaseSinkFactory.class));
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicChunjunSinkDescriptor {
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }


        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.KingBase;
        }
    }

}

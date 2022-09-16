/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.connector.sink;

import com.alibaba.citrus.turbine.Context;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.google.common.collect.Sets;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.sql.Types;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-07 20:11
 **/
public class MySQLSinkFactory extends ChunjunSinkFactory {
    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {
        JdbcUtil.putExtParam(jdbcConf);
    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        return MysqlDialect.class;
    }

//    @Override
//    protected JdbcDialect createJdbcDialect(SyncConf syncConf) {
//        return new MysqlDialect();
//    }
    /**
     * ==========================================================
     * End impl: IStreamTableCreator
     * ===========================================================
     */
    protected String parseType(ISelectedTab.ColMeta cm) {
        return cm.getType().accept(new DataType.TypeVisitor<String>() {
            @Override
            public String bigInt(DataType type) {
                return "BIGINT";
            }

            @Override
            public String doubleType(DataType type) {
                return "DOUBLE";
            }

            @Override
            public String dateType(DataType type) {
                return "DATE";
            }

            @Override
            public String timestampType(DataType type) {
                return "TIMESTAMP";
            }

            @Override
            public String bitType(DataType type) {
                return "BIT";
            }

            @Override
            public String blobType(DataType type) {
                // TINYBLOB、BLOB、MEDIUMBLOB、LONGBLOB
                switch (type.type) {
                    case Types.BLOB:
                        return "BLOB";
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                        return "BINARY";
                    case Types.VARBINARY:
                        return "VARBINARY";
                    default:
                        throw new IllegalStateException("illegal type:" + type.type);
                }
            }

            @Override
            public String varcharType(DataType type) {
                return "VARCHAR";
            }

            @Override
            public String intType(DataType type) {
                return "INT";
            }

            @Override
            public String floatType(DataType type) {
                return "FLOAT";
            }

            @Override
            public String decimalType(DataType type) {
                return "DECIMAL";
            }

            @Override
            public String timeType(DataType type) {
                return "TIME";
            }

            @Override
            public String tinyIntType(DataType dataType) {
                return "TINYINT";
            }

            @Override
            public String smallIntType(DataType dataType) {
                return "SMALLINT";
            }
        });
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(
                //  "tis-sink-hudi-plugin"
                MySQLSinkFactory.class
                // "tis-datax-hudi-plugin"
                // , "com.alibaba.datax.plugin.writer.hudi.HudiConfig"
        ));
    }

    @Override
    protected TISMysqlOutputFormat createChunjunOutputFormat(DataSourceFactory dsFactory) {
        return new TISMysqlOutputFormat(dsFactory);
    }


    @TISExtension
    public static class DefaultDescriptor extends BasicChunjunSinkDescriptor {
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return IEndTypeGetter.EndType.MySQL;
        }
    }
}

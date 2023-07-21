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

package com.qlangtech.tis.hive;

import com.qlangtech.tis.config.authtoken.IKerberosUserToken;
import com.qlangtech.tis.config.authtoken.IUserNamePasswordUserToken;
import com.qlangtech.tis.config.authtoken.IUserTokenVisitor;
import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.config.kerberos.IKerberos;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.jdbc.Utils;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-21 16:35
 **/
public class Hms implements Describable<Hms> {
    // "192.168.28.200:10000";
    @FormField(ordinal = 3, validate = {Validator.require, Validator.host})
    public String hiveAddress;


    @FormField(ordinal = 5, validate = {Validator.require})
    public UserToken userToken;

    public static DataSourceMeta.JDBCConnection createConnection(String jdbcUrl, UserToken userToken) throws Exception {
        HiveDriver hiveDriver = new HiveDriver();
        Properties props = new Properties();
        StringBuffer jdbcUrlBuffer = new StringBuffer(jdbcUrl);
        return userToken.accept(new IUserTokenVisitor<DataSourceMeta.JDBCConnection>() {
            @Override
            public DataSourceMeta.JDBCConnection visit(IUserNamePasswordUserToken ut) throws Exception {
                props.setProperty(Utils.JdbcConnectionParams.AUTH_USER, ut.getUserName());
                props.setProperty(Utils.JdbcConnectionParams.AUTH_PASSWD, ut.getPassword());
                return createConnection(hiveDriver, props, jdbcUrlBuffer);
            }

            @Override
            public DataSourceMeta.JDBCConnection visit(IKerberosUserToken token) throws Exception {
                IKerberos kerberosCfg = token.getKerberosCfg();
                jdbcUrlBuffer.append(";principal=")
                        .append(kerberosCfg.getPrincipal());
                //  .append(";sasl.qop=").append(kerberosCfg.getKeyTabPath().getAbsolutePath());
                HiveConf conf = new HiveConf();

                //  UserGroupInformation.setConfiguration(conf);
                HdfsFileSystemFactory.setConfiguration(token.getKerberosCfg(), conf);
                return createConnection(hiveDriver, props, jdbcUrlBuffer);
            }
        });
    }

    private static DataSourceMeta.JDBCConnection createConnection(
            HiveDriver hiveDriver, Properties props, StringBuffer jdbcUrlBuffer) throws SQLException {
        String jdbcUrl = jdbcUrlBuffer.toString();
        return new DataSourceMeta.JDBCConnection(hiveDriver.connect(jdbcUrl, props), jdbcUrl);
    }


    public DataSourceMeta.JDBCConnection getConnection(String jdbcUrl, String dbName, boolean usingPool) throws SQLException {
        final ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(Hiveserver2DataSourceFactory.class.getClassLoader());
            if (usingPool) {
                return HiveDBUtils.getInstance(this.hiveAddress, dbName, this.userToken).createConnection();
            } else {
                return createConnection(jdbcUrl, this.userToken);

            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(currentLoader);
        }
    }

    @TISExtension
    public static class DftDesc extends Descriptor<Hms> {
        @Override
        public String getDisplayName() {
            return "HMS";
        }
    }
}

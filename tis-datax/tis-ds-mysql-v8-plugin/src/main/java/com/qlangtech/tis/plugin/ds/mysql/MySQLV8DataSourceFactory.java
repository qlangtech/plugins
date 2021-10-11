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

package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.extension.TISExtension;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-08 21:47
 **/
public class MySQLV8DataSourceFactory extends MySQLDataSourceFactory {
    //    static {
//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
    private static final com.mysql.cj.jdbc.Driver mysql8Driver;

    static {
        try {
            mysql8Driver = new com.mysql.cj.jdbc.Driver();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Connection getConnection(String jdbcUrl) throws SQLException {
        Properties props = new Properties();
        props.put("user", StringUtils.trimToNull(this.userName));
        props.put("password", StringUtils.trimToEmpty(password));
        // 为了避开与Mysql5的连接冲突，需要直接从driver中创建connection对象
        return mysql8Driver.connect(jdbcUrl, props);
       //  return DriverManager.getConnection(jdbcUrl, , );
    }

    @TISExtension
    public static class V8Descriptor extends DefaultDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_MYSQL_V8;
        }

//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return super.validate(msgHandler, context, postFormVals);
//        }
    }
}

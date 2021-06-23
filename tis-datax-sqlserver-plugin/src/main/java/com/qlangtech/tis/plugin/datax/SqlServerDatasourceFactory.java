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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-07 09:47
 **/
public class SqlServerDatasourceFactory extends BasicDataSourceFactory {
    private static final String DS_TYPE_SQL_SERVER = "SqlServer";

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        String jdbcUrl = "jdbc:mysql://" + ip + ":" + this.port + "/" + dbName + "?useUnicode=yes";
        if (StringUtils.isNotEmpty(this.encode)) {
            jdbcUrl = jdbcUrl + "&characterEncoding=" + this.encode;
        }
        if (StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl = jdbcUrl + "&" + this.extraParams;
        }
        return jdbcUrl;
    }

    @Override
    public Connection getConnection(String jdbcUrl) throws SQLException {
        return null;
    }

    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_SQL_SERVER;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }


    }
}

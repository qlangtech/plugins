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

package com.qlangtech.tis.plugin.datax.kingbase.mode;

import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseCompatibleMode;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;

import java.util.Optional;

/**
 * KingBase 支持三种数据库兼容模式
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 15:28
 **/
public class MySQLMode extends KingBaseCompatibleMode {
    private static final EndType endType = EndType.MySQL;

    @Override
    public EndType getEndType() {
        return endType;
    }

    @Override
    public Class<?> getJdbcDialectClass() {
        return MysqlDialect.class;
    }

    @Override
    public Optional<String> getEscapeChar() {
        return BasicDataSourceFactory.MYSQL_ESCAPE_COL_CHAR;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<KingBaseCompatibleMode> {
        public DftDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return endType.name();
        }
    }
}

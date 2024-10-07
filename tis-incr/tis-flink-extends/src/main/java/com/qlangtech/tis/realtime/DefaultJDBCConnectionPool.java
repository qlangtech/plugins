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

package com.qlangtech.tis.realtime;


import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-10-05 22:14
 **/
public class DefaultJDBCConnectionPool extends JDBCConnectionPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJDBCConnectionPool.class);

    private final ConcurrentMap<String, JDBCConnection> connectionCache = Maps.newConcurrentMap();

    public DefaultJDBCConnectionPool() {
    }

    @Override
    public JDBCConnection getConnection(String jdbcUrl, boolean verify, Function<String, JDBCConnection> mappingFunction) {
        return connectionCache.computeIfAbsent(jdbcUrl, mappingFunction.andThen((conn) -> {
            return new JDBCConnection(conn.getConnection(), conn.getUrl()) {
                @Override
                public void close() throws SQLException {

                }
            };
        }));
    }

    @Override
    public JDBCConnection getConnection(String jdbcUrl, boolean verify) {
        return connectionCache.get(jdbcUrl);
    }


    @Override
    public void close() throws Exception {
        for (Map.Entry<String, JDBCConnection> entry : connectionCache.entrySet()) {
            try {
                entry.getValue().getConnection().close();
            } catch (SQLException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }
}

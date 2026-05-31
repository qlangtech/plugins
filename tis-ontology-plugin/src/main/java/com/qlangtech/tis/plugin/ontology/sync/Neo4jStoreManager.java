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
package com.qlangtech.tis.plugin.ontology.sync;

import com.qlangtech.tis.manage.common.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.debug_log_enabled;

/**
 * 嵌入式 Neo4j 管理器（单例）。TIS 启动时调用 {@link #getInstance()} 完成初始化。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class Neo4jStoreManager {

    private static final Logger log = LoggerFactory.getLogger(Neo4jStoreManager.class);
    private static final String DB_DIR_NAME = "neo4j-data";

    private static volatile Neo4jStoreManager INSTANCE;

    private final DatabaseManagementService managementService;
    private final GraphDatabaseService database;

    private Neo4jStoreManager() {
        Path dbPath = new File(Config.getDataDir(), DB_DIR_NAME).toPath();
        log.info("[Neo4j] starting embedded Neo4j, data dir: {}", dbPath.toAbsolutePath());

        this.managementService = new DatabaseManagementServiceBuilder(dbPath)
                //.setUserLogProvider(NullLogProvider.getInstance())
                .setConfig(debug_log_enabled,false)
                .build();
        this.database = managementService.database(DEFAULT_DATABASE_NAME);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[Neo4j] JVM shutdown hook triggered, shutting down Neo4j ...");
            shutdown();
        }, "neo4j-shutdown-hook"));

        log.info("[Neo4j] embedded Neo4j started, database: {}", DEFAULT_DATABASE_NAME);
    }

    public static Neo4jStoreManager getInstance() {
        if (INSTANCE == null) {
            synchronized (Neo4jStoreManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new Neo4jStoreManager();
                }
            }
        }
        return INSTANCE;
    }

    public GraphDatabaseService getDatabase() {
        return database;
    }

    private void shutdown() {
        try {
            managementService.shutdown();
            log.info("[Neo4j] database shut down safely.");
        } catch (Exception e) {
            log.error("[Neo4j] error during shutdown", e);
        }
    }
}
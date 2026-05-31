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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 启动时幂等建立唯一性约束 + HNSW 向量索引，并启动周期巡检定时器。
 * 调用入口：TIS 应用启动后调用 {@link #bootstrap()}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class Neo4jBootstrapper {

    private static final Logger log = LoggerFactory.getLogger(Neo4jBootstrapper.class);
    private static final int VECTOR_DIM = OntologyEmbeddingService.EMBEDDING_DIM;
    private static final int PATROL_INTERVAL_MINUTES = 5;

    private final GraphDatabaseService db;
    private final OntologyNeo4jSyncService syncService;

    public Neo4jBootstrapper(GraphDatabaseService db, OntologyNeo4jSyncService syncService) {
        this.db = db;
        this.syncService = syncService;
    }

    /** 执行全部初始化动作：约束 + 索引 + 巡检定时器。 */
    public void bootstrap() {
        createConstraints();
        createVectorIndexes();
        startPatrolScheduler();
    }

    private void createConstraints() {
        String[] stmts = {
            "CREATE CONSTRAINT domain_name_unique    IF NOT EXISTS FOR (d:OntologyDomain)  REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT ot_unique             IF NOT EXISTS FOR (n:ObjectType)      REQUIRE (n.domain, n.name) IS UNIQUE",
            "CREATE CONSTRAINT shared_prop_unique    IF NOT EXISTS FOR (n:SharedProperty)  REQUIRE (n.domain, n.name) IS UNIQUE",
            "CREATE CONSTRAINT value_type_unique     IF NOT EXISTS FOR (n:ValueType)       REQUIRE (n.domain, n.name) IS UNIQUE",
            "CREATE CONSTRAINT glossary_unique       IF NOT EXISTS FOR (n:Glossary)        REQUIRE (n.domain, n.term) IS UNIQUE",
        };
        executeStatements("constraints", stmts);
    }

    private void createVectorIndexes() {
        String optionsTpl = "{indexConfig:{`vector.dimensions`:%d,`vector.similarity_function`:'cosine'}}";
        String options = String.format(optionsTpl, VECTOR_DIM);

        String[] stmts = {
            String.format("CREATE VECTOR INDEX objecttype_embedding_idx  IF NOT EXISTS FOR (n:ObjectType)     ON (n.embedding) OPTIONS %s", options),
            String.format("CREATE VECTOR INDEX property_embedding_idx     IF NOT EXISTS FOR (n:Property)       ON (n.embedding) OPTIONS %s", options),
            String.format("CREATE VECTOR INDEX sharedprop_embedding_idx   IF NOT EXISTS FOR (n:SharedProperty) ON (n.embedding) OPTIONS %s", options),
            String.format("CREATE VECTOR INDEX glossary_embedding_idx      IF NOT EXISTS FOR (n:Glossary)       ON (n.embedding) OPTIONS %s", options),
        };
        executeStatements("vector indexes", stmts);
        awaitIndexesOnline(new String[]{
            "objecttype_embedding_idx", "property_embedding_idx",
            "sharedprop_embedding_idx", "glossary_embedding_idx"
        });
    }

    private void executeStatements(String phase, String[] stmts) {
        try (Transaction tx = db.beginTx()) {
            for (String s : stmts) tx.execute(s);
            tx.commit();
            log.info("[Neo4jBootstrapper] {} created (idempotent)", phase);
        }
    }

    private void awaitIndexesOnline(String[] names) {
        for (String name : names) awaitOne(name);
    }

    private void awaitOne(String name) {
        for (int i = 0; i < 60; i++) {
            try (Transaction tx = db.beginTx()) {
                var result = tx.execute("SHOW INDEXES");
                String state = null;
                while (result.hasNext()) {
                    var row = result.next();
                    if (name.equals(row.get("name"))) { state = (String) row.get("state"); break; }
                }
                tx.commit();
                if ("ONLINE".equalsIgnoreCase(state)) {
                    log.info("[Neo4jBootstrapper] index '{}' ONLINE", name);
                    return;
                }
            }
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
        log.warn("[Neo4jBootstrapper] index '{}' did not reach ONLINE within 30s", name);
    }

    private void startPatrolScheduler() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ontology-neo4j-patrol");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            try {
                syncService.patrolOrphanNodes();
            } catch (Exception e) {
                log.warn("[Neo4jPatrol] patrol error", e);
            }
        }, PATROL_INTERVAL_MINUTES, PATROL_INTERVAL_MINUTES, TimeUnit.MINUTES);
        log.info("[Neo4jBootstrapper] patrol scheduler started, interval={}min", PATROL_INTERVAL_MINUTES);
    }
}
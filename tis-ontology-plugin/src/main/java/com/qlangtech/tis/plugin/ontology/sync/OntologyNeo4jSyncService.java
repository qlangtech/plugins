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

import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT;
import com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty;
import com.qlangtech.tis.plugin.ontology.impl.linker.LinkResources;
import com.qlangtech.tis.plugin.ontology.impl.typeref.DefaultPropertyTypeRef;
import com.qlangtech.tis.plugin.ontology.impl.typeref.SharedPropertyTypeRef;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.qlangtech.tis.plugin.ontology.sync.CypherParams.*;

/**
 * 将本体实体实时同步到 Neo4j 图数据库。
 * 每个 syncXxx 方法均为幂等（MERGE 语义），可安全重复调用。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class OntologyNeo4jSyncService {

    private static final Logger log = LoggerFactory.getLogger(OntologyNeo4jSyncService.class);

    private static volatile OntologyNeo4jSyncService INSTANCE;

    private final GraphDatabaseService db;
    private final OntologyGraphMapper mapper;

    private OntologyNeo4jSyncService(GraphDatabaseService db, OntologyEmbeddingService embeddingService) {
        this.db = db;
        this.mapper = new OntologyGraphMapper(embeddingService);
    }

    public static OntologyNeo4jSyncService getInstance() {
        if (INSTANCE == null) {
            synchronized (OntologyNeo4jSyncService.class) {
                if (INSTANCE == null) {
                    OntologyEmbeddingService emb = new OntologyEmbeddingService();
                    INSTANCE = new OntologyNeo4jSyncService(Neo4jStoreManager.getInstance().getDatabase(), emb);
                    new Neo4jBootstrapper(INSTANCE.db, INSTANCE).bootstrap();
                }
            }
        }
        return INSTANCE;
    }

    // ================================================================
    //  全量重建
    // ================================================================

    /**
     * 清除 domain 子图后全量重建。Domain 首次创建或显式重建时调用。
     */
    public void fullRebuild(String domain) {
        if (StringUtils.isEmpty(domain)) {
            throw new IllegalArgumentException("param domain can not be empty");
        }
        log.info("[Neo4jSync] fullRebuild domain={}", domain);
        clearDomainSubgraph(domain);

        syncDomain(domain);

        for (OntologyObjectType ot : OntologyObjectType.loadAll(domain)) {
            syncObjectType(domain, ot);
        }
        for (OntologyLinker linker : Ontology.loadAllLinkers(domain)) {
            syncLinker(domain, linker);
        }
        for (OntologySharedProperty sp : Ontology.loadAllSharedProperties(domain)) {
            syncSharedProperty(domain, sp);
        }
        for (OntologyValueType vt : Ontology.loadAllObjectTypes(domain)) {
            syncValueType(domain, vt);
        }
        for (OntologyGlossary g : Ontology.loadAllGlossary(domain)) {
            syncGlossary(domain, g);
        }
        log.info("[Neo4jSync] fullRebuild done, domain={}", domain);
    }

    private void clearDomainSubgraph(String domain) {
        if (StringUtils.isEmpty(domain)) {
            throw new IllegalArgumentException("param domain can not be empty");
        }
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MATCH (d:OntologyDomain {name: $domain})-[*0..]->(n)
                    DETACH DELETE n
                    """, Map.of(DOMAIN, domain));
            tx.execute("MATCH (d:OntologyDomain {name: $domain}) DETACH DELETE d",
                    Map.of(DOMAIN, domain));
            tx.commit();
        }
    }

    // ================================================================
    //  增量同步
    // ================================================================

    public void syncDomain(String domain) {
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (d:OntologyDomain {name: $domain})
                    SET d.updatedAt = timestamp()
                    """, Map.of(DOMAIN, domain));
            tx.commit();
        }
    }

    /**
     * 同步 ObjectType 及其所有 Property。
     */
    public void syncObjectType(String domain, OntologyObjectType ot) {
        Map<String, Object> p = mapper.toObjectTypeParams(domain, ot);
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (d:OntologyDomain {name: $domain})
                    MERGE (ot:ObjectType {domain: $domain, name: $name})
                    SET ot.alias        = $alias,
                        ot.boundDsName  = $boundDsName,
                        ot.physicalTable= $physicalTable,
                        ot.embedding    = $embedding,
                        ot.updatedAt    = timestamp()
                    MERGE (d)-[:CONTAINS]->(ot)
                    """, p);
            tx.commit();
        }

        // 先删旧 Property 节点再重建
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MATCH (ot:ObjectType {domain: $domain, name: $otName})-[:HAS_PROPERTY]->(p:Property)
                    DETACH DELETE p
                    """, Map.of(DOMAIN, domain, OT_NAME, ot.getName()));
            tx.commit();
        }

        List<OntologyProperty> cols = ot.getCols();
        for (int i = 0; i < cols.size(); i++) {
            syncProperty(domain, ot.getName(), cols.get(i), i);
        }

        log.debug("[Neo4jSync] synced ObjectType={} with {} properties", ot.getName(), cols.size());
    }

    public void syncProperty(String domain, String otName, OntologyProperty prop, int ordinal) {
        Map<String, Object> p = mapper.toPropertyParams(domain, otName, prop, ordinal);
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (ot:ObjectType {domain: $domain, name: $otName})
                    MERGE (pr:Property {domain: $domain, otName: $otName, name: $name})
                    SET pr.type        = $type,
                        pr.endType     = $endType,
                        pr.pk          = $pk,
                        pr.nullable    = $nullable,
                        pr.description = $description,
                        pr.role        = $role,
                        pr.agg         = $agg,
                        pr.embedding   = $embedding,
                        pr.updatedAt   = timestamp()
                    MERGE (ot)-[:HAS_PROPERTY {ordinal: $ordinal}]->(pr)
                    """, p);

            // Property → ValueType or SharedProperty 关系
            if (prop instanceof com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty dp) {
                if (dp.typeRef instanceof DefaultPropertyTypeRef dtr
                        && dtr.valueType != null && !dtr.valueType.isBlank()) {
                    tx.execute("""
                            MATCH (pr:Property {domain: $domain, otName: $otName, name: $propName}),
                                  (vt:ValueType {domain: $domain, name: $vtName})
                            MERGE (pr)-[:USES_VALUE_TYPE]->(vt)
                            """, Map.of(DOMAIN, domain, OT_NAME, otName,
                            PROP_NAME, prop.getName(), VT_NAME, dtr.valueType));
                } else if (dp.typeRef instanceof SharedPropertyTypeRef spr) {
                    tx.execute("""
                            MATCH (pr:Property {domain: $domain, otName: $otName, name: $propName}),
                                  (sp:SharedProperty {domain: $domain, name: $spName})
                            MERGE (pr)-[:REFS_SHARED_PROPERTY]->(sp)
                            """, Map.of(DOMAIN, domain, OT_NAME, otName,
                            PROP_NAME, prop.getName(), SP_NAME, spr.sharedPropRef));
                }
            }
            tx.commit();
        }
    }

    /**
     * Linker 折叠为 LINKED_TO 关系（先删旧关系）。
     */
    public void syncLinker(String domain, OntologyLinker linker) {
        String linkerName = linker.identityValue();
        String relType = linker.getLinkTypeEnd().getVal();

        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MATCH ()-[r:LINKED_TO {linkerName: $linkerName, domain: $domain}]->()
                    DELETE r
                    """, Map.of(LINKER_NAME, linkerName, DOMAIN, domain));
            tx.commit();
        }

        LinkResources.ObjectLinkerPair pair = linker.getLinkResourcesStep().getLinks();
        for (LinkResources.ObjectLinkInfo link : List.of(pair.left(), pair.right())) {
            try (Transaction tx = db.beginTx()) {
                tx.execute("""
                        MATCH (src:ObjectType {domain: $domain, name: $source}),
                              (tgt:ObjectType {domain: $domain, name: $target})
                        MERGE (src)-[r:LINKED_TO {linkerName: $linkerName, domain: $domain}]->(tgt)
                        SET r.relType      = $relType,
                            r.cardinality  = $cardinality,
                            r.sourceField  = $sourceField,
                            r.targetField  = $targetField,
                            r.updatedAt    = timestamp()
                        """, Map.of(
                        DOMAIN, domain,
                        SOURCE, link.source(),
                        TARGET, link.target(),
                        LINKER_NAME, linkerName,
                        REL_TYPE, relType,
                        CARDINALITY, link.cardinality() == null ? "" : link.cardinality().name(),
                        SOURCE_FIELD, nullSafe(link.sourceField()),
                        TARGET_FIELD, nullSafe(link.targetField())
                ));
                tx.commit();
            }
        }
        log.debug("[Neo4jSync] synced Linker={}", linkerName);
    }

    public void syncSharedProperty(String domain, OntologySharedProperty sp) {
        Map<String, Object> p = mapper.toSharedPropertyParams(domain, sp);
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (d:OntologyDomain {name: $domain})
                    MERGE (sp:SharedProperty {domain: $domain, name: $name})
                    SET sp.alias       = $alias,
                        sp.description = $description,
                        sp.type        = $type,
                        sp.embedding   = $embedding,
                        sp.updatedAt   = timestamp()
                    MERGE (d)-[:CONTAINS]->(sp)
                    """, p);
            tx.commit();
        }
    }

    public void syncValueType(String domain, OntologyValueType vt) {
        Map<String, Object> p = mapper.toValueTypeParams(domain, vt);
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (d:OntologyDomain {name: $domain})
                    MERGE (vt:ValueType {domain: $domain, name: $name})
                    SET vt.description      = $description,
                        vt.ontologyType     = $ontologyType,
                        vt.constraintKind   = $constraintKind,
                        vt.constraintParams = $constraintParams,
                        vt.updatedAt        = timestamp()
                    MERGE (d)-[:CONTAINS]->(vt)
                    """, p);
            tx.commit();
        }
    }

    /**
     * 同步 Glossary 节点及其指向 OT / Property 的关系。
     */
    public void syncGlossary(String domain, OntologyGlossary g) {
        Map<String, Object> p = mapper.toGlossaryParams(domain, g);
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MERGE (d:OntologyDomain {name: $domain})
                    MERGE (gl:Glossary {domain: $domain, term: $term})
                    SET gl.synonyms    = $synonyms,
                        gl.description = $description,
                        gl.targetKind  = $targetKind,
                        gl.targetRef   = $targetRef,
                        gl.metricSql   = $metricSql,
                        gl.embedding   = $embedding,
                        gl.updatedAt   = timestamp()
                    MERGE (d)-[:CONTAINS]->(gl)
                    """, p);

            tx.execute("""
                    MATCH (gl:Glossary {domain: $domain, term: $term})-[r:TARGETS_OT|TARGETS_PROP]->()
                    DELETE r
                    """, Map.of(DOMAIN, domain, TERM, g.term));

            if (g.target instanceof GlossaryTargetOT tot) {
                tx.execute("""
                        MATCH (gl:Glossary {domain: $domain, term: $term}),
                              (ot:ObjectType {domain: $domain, name: $otName})
                        MERGE (gl)-[:TARGETS_OT]->(ot)
                        """, Map.of(DOMAIN, domain, TERM, g.term, OT_NAME, tot.objectType));
            } else if (g.target instanceof GlossaryTargetProperty tp) {
                tx.execute("""
                        MATCH (gl:Glossary {domain: $domain, term: $term}),
                              (pr:Property {domain: $domain, otName: $otName, name: $propName})
                        MERGE (gl)-[:TARGETS_PROP]->(pr)
                        """, Map.of(DOMAIN, domain, TERM, g.term,
                        OT_NAME, tp.objectType, PROP_NAME, tp.targetField));
            }
            // GlossaryTargetMetricExpr: sql 已存在 gl.metricSql，无需额外关系

            tx.commit();
        }
    }

    // ================================================================
    //  删除孤立节点（巡检用）
    // ================================================================

    public void patrolOrphanNodes() {
        log.debug("[Neo4jPatrol] starting orphan node patrol ...");
        for (Pair<OntologyDomain, ?> pair : OntologyDomain.getDoaminList()) {
            String domain = pair.getKey().name;
            patrolDomain(domain);
        }
    }

    private void patrolDomain(String domain) {
        List<String> xmlOtNames = OntologyObjectType.loadAll(domain)
                .stream().map(OntologyObjectType::getName).toList();

        try (Transaction tx = db.beginTx()) {
            var result = tx.execute("""
                    MATCH (ot:ObjectType {domain: $domain}) RETURN ot.name AS name
                    """, Map.of(DOMAIN, domain));
            while (result.hasNext()) {
                String name = (String) result.next().get(NAME);
                if (!xmlOtNames.contains(name)) {
                    deleteObjectTypeSubgraph(domain, name);
                    log.info("[Neo4jPatrol] deleted orphan ObjectType={} in domain={}", name, domain);
                }
            }
            tx.commit();
        }
    }

    private void deleteObjectTypeSubgraph(String domain, String otName) {
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    MATCH (ot:ObjectType {domain: $domain, name: $name})
                    OPTIONAL MATCH (ot)-[:HAS_PROPERTY]->(p:Property)
                    DETACH DELETE p, ot
                    """, Map.of(DOMAIN, domain, NAME, otName));
            tx.commit();
        }
    }

    public void deleteNode(String domain, String label, String nameOrTerm) {
        String key = "Glossary".equals(label) ? TERM : NAME;
        try (Transaction tx = db.beginTx()) {
            tx.execute(String.format("MATCH (n:%s {domain: $domain, %s: $val}) DETACH DELETE n", label, key),
                    Map.of(DOMAIN, domain, VAL, nameOrTerm));
            tx.commit();
        }
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }
}
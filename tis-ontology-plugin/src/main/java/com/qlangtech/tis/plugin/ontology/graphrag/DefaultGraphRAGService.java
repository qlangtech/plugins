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
package com.qlangtech.tis.plugin.ontology.graphrag;

import com.qlangtech.tis.plugin.ontology.sync.Neo4jStoreManager;
import com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * GraphRAG 检索服务实现。Neo4j 内一次完成 4 路向量召回 + 词典匹配 + 子图扩展。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
public class DefaultGraphRAGService implements GraphRAGService {

    private static final Logger log = LoggerFactory.getLogger(DefaultGraphRAGService.class);

    private static volatile DefaultGraphRAGService INSTANCE;

    private final GraphDatabaseService db;
    private final OntologyEmbeddingService embeddingService;
    private final ExecutorService recallPool;

    private DefaultGraphRAGService(GraphDatabaseService db, OntologyEmbeddingService embeddingService) {
        this.db = db;
        this.embeddingService = embeddingService;
        this.recallPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "graphrag-recall");
            t.setDaemon(true);
            return t;
        });
    }

    public static DefaultGraphRAGService getInstance() {
        if (INSTANCE == null) {
            synchronized (DefaultGraphRAGService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultGraphRAGService(
                            Neo4jStoreManager.getInstance().getDatabase(),
                            new OntologyEmbeddingService());
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public RetrievalResult retrieve(String domain, String nlq, RetrievalOptions opts) {
        if (StringUtils.isBlank(domain) || StringUtils.isBlank(nlq)) {
            return new RetrievalResult("", List.of(), List.of(), List.of());
        }
        if (opts == null) opts = RetrievalOptions.defaults();

        long t0 = System.currentTimeMillis();
        List<EntrySeed> seeds = recallSeeds(domain, nlq, opts);
        if (seeds.isEmpty()) {
            log.info("[GraphRAG] domain={} nlq=\"{}\" no seed hit", domain, nlq);
            return new RetrievalResult("", List.of(), List.of(), List.of());
        }

        SubgraphSnapshot snap = expandSubgraph(domain, seeds, opts);
        String prompt = PromptSerializer.serialize(snap, opts);

        long elapsed = System.currentTimeMillis() - t0;
        log.info("[GraphRAG] domain={} seeds={} ots={} linkers={} elapsed={}ms",
                domain, seeds.size(), snap.objectTypes.size(), snap.linkers.size(), elapsed);

        return new RetrievalResult(
                prompt,
                List.copyOf(snap.objectTypes.keySet()),
                List.copyOf(snap.linkers),
                snap.glossaries.stream().map(g -> g.term).collect(Collectors.toList())
        );
    }

    // ──────────────────────── Step 1：入口实体识别 ────────────────────────

    private List<EntrySeed> recallSeeds(String domain, String nlq, RetrievalOptions opts) {
        List<String> tokens = SimpleTokenizer.tokenize(nlq);
        List<Float> queryVec = embeddingService.embedAsList(nlq);
        int topK = opts.topKSeeds();

        CompletableFuture<List<EntrySeed>> dictTask = CompletableFuture.supplyAsync(
                () -> recallByDictionary(domain, tokens), recallPool);
        CompletableFuture<List<EntrySeed>> otTask = CompletableFuture.supplyAsync(
                () -> recallByVector(domain, "objecttype_embedding_idx", "ObjectType",
                        EntrySeed.SeedKind.ObjectType, "vec_ot", queryVec, topK), recallPool);
        CompletableFuture<List<EntrySeed>> propTask = CompletableFuture.supplyAsync(
                () -> recallByVector(domain, "property_embedding_idx", "Property",
                        EntrySeed.SeedKind.Property, "vec_prop", queryVec, topK), recallPool);
        CompletableFuture<List<EntrySeed>> spTask = CompletableFuture.supplyAsync(
                () -> recallByVector(domain, "sharedprop_embedding_idx", "SharedProperty",
                        EntrySeed.SeedKind.SharedProperty, "vec_sp", queryVec, topK), recallPool);
        CompletableFuture<List<EntrySeed>> glossTask = CompletableFuture.supplyAsync(
                () -> recallByVector(domain, "glossary_embedding_idx", "Glossary",
                        EntrySeed.SeedKind.Glossary, "vec_gloss", queryVec, topK), recallPool);

        try {
            CompletableFuture.allOf(dictTask, otTask, propTask, spTask, glossTask).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } catch (ExecutionException e) {
            log.warn("[GraphRAG] recall error", e.getCause());
            return List.of();
        }

        List<EntrySeed> merged = new ArrayList<>();
        merged.addAll(dictTask.join());
        merged.addAll(otTask.join());
        merged.addAll(propTask.join());
        merged.addAll(spTask.join());
        merged.addAll(glossTask.join());

        if (merged.isEmpty()) {
            merged.addAll(recallByKeywordFallback(domain, tokens));
        }

        return dedupAndRank(merged);
    }

    private List<EntrySeed> recallByDictionary(String domain, List<String> tokens) {
        if (tokens.isEmpty()) return List.of();
        List<EntrySeed> out = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    MATCH (g:Glossary {domain: $domain})
                    WHERE g.term IN $tokens
                       OR ANY(s IN g.synonyms WHERE s IN $tokens)
                    RETURN g.term AS term
                    """, Map.of("domain", domain, "tokens", tokens));
            while (rs.hasNext()) {
                String term = (String) rs.next().get("term");
                out.add(new EntrySeed(EntrySeed.SeedKind.Glossary, null, term, 1.0, "dict"));
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] dictionary recall error", e);
        }
        return out;
    }

    private List<EntrySeed> recallByVector(String domain, String indexName, String label,
                                           EntrySeed.SeedKind kind, String source,
                                           List<Float> queryVec, int topK) {
        List<EntrySeed> out = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    CALL db.index.vector.queryNodes($idx, $topK, $vec)
                    YIELD node, score
                    WHERE node.domain = $domain
                    RETURN node, score
                    """, Map.of("idx", indexName, "topK", topK, "vec", queryVec, "domain", domain));
            while (rs.hasNext()) {
                Map<String, Object> row = rs.next();
                org.neo4j.graphdb.Node n = (org.neo4j.graphdb.Node) row.get("node");
                double score = ((Number) row.get("score")).doubleValue();
                String name;
                String otName = null;
                if (kind == EntrySeed.SeedKind.Glossary) {
                    name = (String) n.getProperty("term", "");
                } else if (kind == EntrySeed.SeedKind.Property) {
                    name = (String) n.getProperty("name", "");
                    otName = (String) n.getProperty("otName", "");
                } else {
                    name = (String) n.getProperty("name", "");
                }
                if (StringUtils.isNotBlank(name)) {
                    out.add(new EntrySeed(kind, otName, name, score, source));
                }
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] vector recall error idx={}", indexName, e);
        }
        return out;
    }

    private List<EntrySeed> recallByKeywordFallback(String domain, List<String> tokens) {
        if (tokens.isEmpty()) return List.of();
        List<EntrySeed> out = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    MATCH (ot:ObjectType {domain: $domain})
                    WHERE ANY(t IN $tokens WHERE
                              toLower(ot.name)  CONTAINS toLower(t)
                           OR toLower(ot.alias) CONTAINS toLower(t))
                    RETURN ot.name AS name LIMIT 10
                    """, Map.of("domain", domain, "tokens", tokens));
            while (rs.hasNext()) {
                String name = (String) rs.next().get("name");
                out.add(new EntrySeed(EntrySeed.SeedKind.ObjectType, null, name, 0.5, "keyword"));
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] keyword fallback error", e);
        }
        return out;
    }

    private List<EntrySeed> dedupAndRank(List<EntrySeed> raw) {
        Map<String, EntrySeed> best = new LinkedHashMap<>();
        for (EntrySeed s : raw) {
            best.merge(s.key(), s, (a, b) -> a.score() >= b.score() ? a : b);
        }
        List<EntrySeed> sorted = new ArrayList<>(best.values());
        sorted.sort(Comparator.comparingDouble(EntrySeed::score).reversed());
        return sorted;
    }

    // ──────────────────────── Step 2：子图扩展 ────────────────────────

    private SubgraphSnapshot expandSubgraph(String domain, List<EntrySeed> seeds, RetrievalOptions opts) {
        SubgraphSnapshot snap = new SubgraphSnapshot();
        Map<String, Double> otScores = new HashMap<>();

        Set<String> seedOts = new HashSet<>();
        Set<String> seedSps = new HashSet<>();
        Set<String> seedGlossaries = new HashSet<>();
        Map<String, Double> propScoreByOtAndName = new HashMap<>();

        for (EntrySeed s : seeds) {
            switch (s.kind()) {
                case ObjectType -> {
                    seedOts.add(s.name());
                    otScores.merge(s.name(), s.score(), Math::max);
                }
                case Property -> {
                    if (StringUtils.isNotBlank(s.otName())) {
                        seedOts.add(s.otName());
                        otScores.merge(s.otName(), s.score() * 0.9, Math::max);
                        propScoreByOtAndName.merge(s.otName() + "\0" + s.name(), s.score(), Math::max);
                    }
                }
                case SharedProperty -> seedSps.add(s.name());
                case Glossary -> seedGlossaries.add(s.name());
            }
        }

        // 通过 Glossary.targetRef 反查 OT / Property
        if (!seedGlossaries.isEmpty()) {
            try (Transaction tx = db.beginTx()) {
                Result rs = tx.execute("""
                        MATCH (g:Glossary {domain: $domain})
                        WHERE g.term IN $terms
                        OPTIONAL MATCH (g)-[:TARGETS_OT]->(ot:ObjectType)
                        OPTIONAL MATCH (g)-[:TARGETS_PROP]->(p:Property)<-[:HAS_PROPERTY]-(ot2:ObjectType)
                        RETURN g.term AS term, g.synonyms AS synonyms, g.description AS description,
                               g.targetKind AS targetKind, g.targetRef AS targetRef, g.metricSql AS metricSql,
                               ot.name AS otName, ot2.name AS ot2Name, p.name AS propName
                        """, Map.of("domain", domain, "terms", new ArrayList<>(seedGlossaries)));
                while (rs.hasNext()) {
                    Map<String, Object> row = rs.next();
                    String term = (String) row.get("term");
                    List<String> synonyms = toStringList(row.get("synonyms"));
                    snap.glossaries.add(new SubgraphSnapshot.GlossaryNode(
                            term, synonyms, (String) row.get("description"),
                            (String) row.get("targetKind"), (String) row.get("targetRef"),
                            (String) row.get("metricSql"), 1.0));
                    String otName = (String) row.get("otName");
                    if (StringUtils.isNotBlank(otName)) {
                        seedOts.add(otName);
                        otScores.merge(otName, 0.95, Math::max);
                    }
                    String ot2Name = (String) row.get("ot2Name");
                    String propName = (String) row.get("propName");
                    if (StringUtils.isNotBlank(ot2Name)) {
                        seedOts.add(ot2Name);
                        otScores.merge(ot2Name, 0.9, Math::max);
                        if (StringUtils.isNotBlank(propName)) {
                            propScoreByOtAndName.merge(ot2Name + "\0" + propName, 0.95, Math::max);
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                log.warn("[GraphRAG] glossary expansion error", e);
            }
        }

        if (seedOts.isEmpty()) {
            // 仅命中 SharedProperty / Glossary(MetricExpr) 的场景
            loadSharedProperties(domain, seedSps, snap);
            return snap;
        }

        // BFS 沿 LINKED_TO 扩展，最多 maxHops 跳
        Set<String> reachableOts = new LinkedHashSet<>(seedOts);
        if (opts.maxHops() > 0) {
            try (Transaction tx = db.beginTx()) {
                Result rs = tx.execute("""
                        MATCH (seed:ObjectType) WHERE seed.domain = $domain AND seed.name IN $seeds
                        CALL {
                            WITH seed
                            MATCH path = (seed)-[:LINKED_TO*1..%d]-(other:ObjectType {domain: $domain})
                            RETURN other
                        }
                        RETURN DISTINCT other.name AS name
                        """.formatted(Math.min(opts.maxHops(), 3)),
                        Map.of("domain", domain, "seeds", new ArrayList<>(seedOts)));
                while (rs.hasNext()) {
                    String otName = (String) rs.next().get("name");
                    reachableOts.add(otName);
                    otScores.putIfAbsent(otName, 0.5);
                }
                tx.commit();
            } catch (Exception e) {
                log.warn("[GraphRAG] hop expansion error", e);
            }
        }

        // 加载 OT + Property + ValueType
        loadObjectTypesAndProperties(domain, reachableOts, otScores, propScoreByOtAndName, snap);

        // 加载 Linker 关系（限定 source/target 都在 reachableOts 里）
        loadLinkers(domain, reachableOts, snap);

        // 加载 SharedProperty（既包含 seed 也包含 Property 引用到的）
        Set<String> spNames = new HashSet<>(seedSps);
        for (SubgraphSnapshot.ObjectTypeNode ot : snap.objectTypes.values()) {
            for (SubgraphSnapshot.PropertyNode p : ot.properties) {
                if (StringUtils.isNotBlank(p.sharedPropRef)) spNames.add(p.sharedPropRef);
            }
        }
        loadSharedProperties(domain, spNames, snap);

        return snap;
    }

    private void loadObjectTypesAndProperties(String domain, Set<String> otNames,
                                              Map<String, Double> otScores,
                                              Map<String, Double> propScoreByOtAndName,
                                              SubgraphSnapshot snap) {
        if (otNames.isEmpty()) return;
        List<String> sortedOts = new ArrayList<>(otNames);
        sortedOts.sort(Comparator.comparingDouble((String n) -> otScores.getOrDefault(n, 0.0)).reversed());

        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    MATCH (ot:ObjectType {domain: $domain}) WHERE ot.name IN $names
                    OPTIONAL MATCH (ot)-[hp:HAS_PROPERTY]->(p:Property)
                    OPTIONAL MATCH (p)-[:USES_VALUE_TYPE]->(vt:ValueType)
                    OPTIONAL MATCH (p)-[:REFS_SHARED_PROPERTY]->(sp:SharedProperty)
                    RETURN ot.name AS otName, ot.alias AS otAlias,
                           ot.physicalTable AS physicalTable, ot.boundDsName AS boundDs,
                           p.name AS pName, p.type AS pType, p.role AS pRole, p.agg AS pAgg,
                           p.pk AS pPk, p.nullable AS pNullable, p.description AS pDesc,
                           p.physicalExpr AS pPhysicalExpr,
                           hp.ordinal AS ordinal,
                           vt.name AS vtName, vt.constraintKind AS vtKind, vt.constraintParams AS vtParams,
                           sp.name AS spName
                    ORDER BY ot.name, hp.ordinal
                    """, Map.of("domain", domain, "names", sortedOts));

            while (rs.hasNext()) {
                Map<String, Object> row = rs.next();
                String otName = (String) row.get("otName");
                SubgraphSnapshot.ObjectTypeNode otNode = snap.objectTypes.computeIfAbsent(otName,
                        n -> new SubgraphSnapshot.ObjectTypeNode(
                                n, (String) row.get("otAlias"), null,
                                (String) row.get("physicalTable"), (String) row.get("boundDs"),
                                otScores.getOrDefault(n, 0.0)));
                String pName = (String) row.get("pName");
                if (pName == null) continue;

                double pScore = propScoreByOtAndName.getOrDefault(otName + "\0" + pName, 0.0);
                otNode.properties.add(new SubgraphSnapshot.PropertyNode(
                        pName,
                        (String) row.get("pType"),
                        (String) row.get("pRole"),
                        (String) row.get("pAgg"),
                        Boolean.TRUE.equals(row.get("pPk")),
                        Boolean.TRUE.equals(row.get("pNullable")),
                        (String) row.get("pDesc"),
                        (String) row.get("pPhysicalExpr"),
                        (String) row.get("vtName"),
                        (String) row.get("spName"),
                        (String) row.get("vtKind"),
                        (String) row.get("vtParams"),
                        pScore));
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] OT/Property load error", e);
        }
    }

    private void loadLinkers(String domain, Set<String> reachableOts, SubgraphSnapshot snap) {
        if (reachableOts.size() < 2) return;
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    MATCH (s:ObjectType {domain: $domain})-[r:LINKED_TO]->(t:ObjectType {domain: $domain})
                    WHERE s.name IN $names AND t.name IN $names
                    RETURN r.linkerName AS linkerName, r.relType AS relType,
                           r.cardinality AS cardinality, r.sourceField AS sourceField,
                           r.targetField AS targetField, s.name AS source, t.name AS target
                    """, Map.of("domain", domain, "names", new ArrayList<>(reachableOts)));
            while (rs.hasNext()) {
                Map<String, Object> row = rs.next();
                snap.linkers.add(new LinkerInfo(
                        (String) row.get("linkerName"),
                        (String) row.get("relType"),
                        (String) row.get("cardinality"),
                        (String) row.get("source"),
                        (String) row.get("sourceField"),
                        (String) row.get("target"),
                        (String) row.get("targetField")
                ));
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] linker load error", e);
        }
    }

    private void loadSharedProperties(String domain, Set<String> names, SubgraphSnapshot snap) {
        if (names.isEmpty()) return;
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute("""
                    MATCH (sp:SharedProperty {domain: $domain}) WHERE sp.name IN $names
                    RETURN sp.name AS name, sp.alias AS alias, sp.type AS type, sp.description AS description
                    """, Map.of("domain", domain, "names", new ArrayList<>(names)));
            while (rs.hasNext()) {
                Map<String, Object> row = rs.next();
                String name = (String) row.get("name");
                snap.sharedProperties.put(name, new SubgraphSnapshot.SharedPropertyNode(
                        name, (String) row.get("alias"),
                        (String) row.get("type"), (String) row.get("description")));
            }
            tx.commit();
        } catch (Exception e) {
            log.warn("[GraphRAG] sharedProperty load error", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object raw) {
        if (raw == null) return List.of();
        if (raw instanceof List<?> l) {
            return ((List<Object>) l).stream().map(String::valueOf).collect(Collectors.toList());
        }
        if (raw instanceof String[] arr) return List.of(arr);
        if (raw instanceof Object[] arr) {
            List<String> out = new ArrayList<>(arr.length);
            for (Object o : arr) out.add(String.valueOf(o));
            return out;
        }
        return List.of(String.valueOf(raw));
    }
}

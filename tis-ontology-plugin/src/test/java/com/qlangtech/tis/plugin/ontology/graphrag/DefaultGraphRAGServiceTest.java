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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * DefaultGraphRAGService.retrieve() 方法的单元测试。
 *
 * 测试策略：专注测试输入验证和边界条件。完整的检索流程需要 Neo4j 集成测试。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
public class DefaultGraphRAGServiceTest {

    @Test
    public void testRetrieve_EmptyDomain_ReturnsEmptyResult() {
        org.neo4j.graphdb.GraphDatabaseService mockDb = org.mockito.Mockito.mock(org.neo4j.graphdb.GraphDatabaseService.class);
        com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService mockEmbedding =
                org.mockito.Mockito.mock(com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService.class);

        DefaultGraphRAGService service = createTestInstance(mockDb, mockEmbedding);
        RetrievalResult result = service.retrieve("", "查询订单", null);

        assertNotNull(result);
        assertEquals("", result.promptContext());
        assertTrue(result.objectTypes().isEmpty());
        assertTrue(result.linkers().isEmpty());
        assertTrue(result.glossaryTerms().isEmpty());
    }

    @Test
    public void testRetrieve_EmptyNlq_ReturnsEmptyResult() {
        org.neo4j.graphdb.GraphDatabaseService mockDb = org.mockito.Mockito.mock(org.neo4j.graphdb.GraphDatabaseService.class);
        com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService mockEmbedding =
                org.mockito.Mockito.mock(com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService.class);

        DefaultGraphRAGService service = createTestInstance(mockDb, mockEmbedding);
        RetrievalResult result = service.retrieve("ecommerce", "", null);

        assertNotNull(result);
        assertEquals("", result.promptContext());
        assertTrue(result.objectTypes().isEmpty());
    }

    @Test
    public void testRetrieve_NullNlq_ReturnsEmptyResult() {
        org.neo4j.graphdb.GraphDatabaseService mockDb = org.mockito.Mockito.mock(org.neo4j.graphdb.GraphDatabaseService.class);
        com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService mockEmbedding =
                org.mockito.Mockito.mock(com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService.class);

        DefaultGraphRAGService service = createTestInstance(mockDb, mockEmbedding);
        RetrievalResult result = service.retrieve("ecommerce", null, null);

        assertNotNull(result);
        assertEquals("", result.promptContext());
    }

    @Test
    public void testRetrieve_NullOptions_UsesDefaults() {
        org.neo4j.graphdb.GraphDatabaseService mockDb = org.mockito.Mockito.mock(org.neo4j.graphdb.GraphDatabaseService.class);
        com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService mockEmbedding =
                org.mockito.Mockito.mock(com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService.class);

        org.neo4j.graphdb.Transaction mockTx = org.mockito.Mockito.mock(org.neo4j.graphdb.Transaction.class);
        org.neo4j.graphdb.Result emptyResult = org.mockito.Mockito.mock(org.neo4j.graphdb.Result.class);
        org.mockito.Mockito.when(mockDb.beginTx()).thenReturn(mockTx);
        org.mockito.Mockito.when(mockTx.execute(org.mockito.ArgumentMatchers.anyString())).thenReturn(emptyResult);
        org.mockito.Mockito.when(mockTx.execute(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap())).thenReturn(emptyResult);
        org.mockito.Mockito.when(emptyResult.hasNext()).thenReturn(false);

        java.util.List<Float> mockVec = new java.util.ArrayList<>();
        for (int i = 0; i < 384; i++) mockVec.add(0.1f);
        org.mockito.Mockito.when(mockEmbedding.embedAsList(org.mockito.ArgumentMatchers.anyString())).thenReturn(mockVec);

        DefaultGraphRAGService service = createTestInstance(mockDb, mockEmbedding);
        RetrievalResult result = service.retrieve("ecommerce", "查询订单", null);

        assertNotNull(result);
    }

    @Test
    public void testRetrievalOptions_Defaults() {
        RetrievalOptions opts = RetrievalOptions.defaults();
        assertEquals(5, opts.topKSeeds());
        assertEquals(2, opts.maxHops());
        assertEquals(3000, opts.tokenBudget());
        assertFalse(opts.includeValueExamples());
    }

    @Test
    public void testRetrievalResult_Construction() {
        String prompt = "## ObjectTypes\n### Order\n";
        List<String> ots = List.of("Order", "Customer");
        List<LinkerInfo> linkers = List.of(
                new LinkerInfo("OrderCustomer", "MANY_TO_ONE", "N:1",
                        "Order", "customerId", "Customer", "id")
        );
        List<String> glossary = List.of("订单", "客户");

        RetrievalResult result = new RetrievalResult(prompt, ots, linkers, glossary);

        assertEquals(prompt, result.promptContext());
        assertEquals(2, result.objectTypes().size());
        assertEquals(1, result.linkers().size());
        assertEquals(2, result.glossaryTerms().size());
    }

    private DefaultGraphRAGService createTestInstance(
            org.neo4j.graphdb.GraphDatabaseService db,
            com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService embedding) {
        try {
            java.lang.reflect.Constructor<DefaultGraphRAGService> constructor =
                    DefaultGraphRAGService.class.getDeclaredConstructor(
                            org.neo4j.graphdb.GraphDatabaseService.class,
                            com.qlangtech.tis.plugin.ontology.sync.OntologyEmbeddingService.class);
            constructor.setAccessible(true);
            return constructor.newInstance(db, embedding);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test instance", e);
        }
    }
}

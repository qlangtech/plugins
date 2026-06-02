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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Step2 扩展后的子图数据容器：以 OT 为主键聚合 Property，独立携带 Linker / Glossary / SharedProperty。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
final class SubgraphSnapshot {

    /**
     * OT 节点（map key 为 OT 名，保持插入序）。
     */
    final Map<String, ObjectTypeNode> objectTypes = new LinkedHashMap<>();
    /**
     * 共享列。
     */
    final Map<String, SharedPropertyNode> sharedProperties = new LinkedHashMap<>();
    /**
     * 命中的 LINKED_TO 关系。
     */
    final List<LinkerInfo> linkers = new ArrayList<>();
    /**
     * 命中术语。
     */
    final List<GlossaryNode> glossaries = new ArrayList<>();

    static final class ObjectTypeNode {
        final String name;
        final String alias;
        final String description;
        final String physicalTable;
        final String boundDsName;
        final double score;
        final List<PropertyNode> properties = new ArrayList<>();

        ObjectTypeNode(String name, String alias, String description,
                       String physicalTable, String boundDsName, double score) {
            this.name = name;
            this.alias = alias;
            this.description = description;
            this.physicalTable = physicalTable;
            this.boundDsName = boundDsName;
            this.score = score;
        }
    }

    static final class PropertyNode {
        final String name;
        final String type;
        final String role;
        final String agg;
        final boolean pk;
        final boolean nullable;
        final String description;
        final String physicalExpr;
        final String valueTypeRef;
        final String sharedPropRef;
        final String constraintKind;
        final String constraintParams;
        final double score;

        PropertyNode(String name, String type, String role, String agg, boolean pk, boolean nullable,
                     String description, String physicalExpr, String valueTypeRef, String sharedPropRef,
                     String constraintKind, String constraintParams, double score) {
            this.name = name;
            this.type = type;
            this.role = role;
            this.agg = agg;
            this.pk = pk;
            this.nullable = nullable;
            this.description = description;
            this.physicalExpr = physicalExpr;
            this.valueTypeRef = valueTypeRef;
            this.sharedPropRef = sharedPropRef;
            this.constraintKind = constraintKind;
            this.constraintParams = constraintParams;
            this.score = score;
        }
    }

    static final class SharedPropertyNode {
        final String name;
        final String alias;
        final String type;
        final String description;

        SharedPropertyNode(String name, String alias, String type, String description) {
            this.name = name;
            this.alias = alias;
            this.type = type;
            this.description = description;
        }
    }

    static final class GlossaryNode {
        final String term;
        final List<String> synonyms;
        final String description;
        final String targetKind;
        final String targetRef;
        final String metricSql;
        final double score;

        GlossaryNode(String term, List<String> synonyms, String description,
                     String targetKind, String targetRef, String metricSql, double score) {
            this.term = term;
            this.synonyms = synonyms;
            this.description = description;
            this.targetKind = targetKind;
            this.targetRef = targetRef;
            this.metricSql = metricSql;
            this.score = score;
        }
    }
}

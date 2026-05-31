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

/**
 * Cypher 参数 Map 的 key 常量，供 {@link OntologyGraphMapper} 和
 * {@link OntologyNeo4jSyncService} 统一引用，避免魔法字符串散落各处。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public final class CypherParams {

    private CypherParams() {}

    // ── 通用 ──────────────────────────────────────────────
    public static final String DOMAIN         = "domain";
    public static final String NAME           = "name";
    public static final String ALIAS          = "alias";
    public static final String DESCRIPTION    = "description";
    public static final String TYPE           = "type";
    public static final String EMBEDDING      = "embedding";

    // ── ObjectType ────────────────────────────────────────
    public static final String BOUND_DS_NAME  = "boundDsName";
    public static final String PHYSICAL_TABLE = "physicalTable";

    // ── Property ──────────────────────────────────────────
    public static final String OT_NAME        = "otName";
    public static final String END_TYPE       = "endType";
    public static final String PK             = "pk";
    public static final String NULLABLE       = "nullable";
    public static final String ORDINAL        = "ordinal";
    public static final String ROLE           = "role";
    public static final String AGG            = "agg";
    public static final String PROP_NAME      = "propName";
    public static final String VT_NAME        = "vtName";
    public static final String SP_NAME        = "spName";

    // ── ValueType ─────────────────────────────────────────
    public static final String ONTOLOGY_TYPE      = "ontologyType";
    public static final String CONSTRAINT_KIND    = "constraintKind";
    public static final String CONSTRAINT_PARAMS  = "constraintParams";

    // ── Glossary ──────────────────────────────────────────
    public static final String TERM        = "term";
    public static final String SYNONYMS    = "synonyms";
    public static final String TARGET_KIND = "targetKind";
    public static final String TARGET_REF  = "targetRef";
    public static final String METRIC_SQL  = "metricSql";

    // ── Linker ────────────────────────────────────────────
    public static final String LINKER_NAME   = "linkerName";
    public static final String REL_TYPE      = "relType";
    public static final String CARDINALITY   = "cardinality";
    public static final String SOURCE        = "source";
    public static final String TARGET        = "target";
    public static final String SOURCE_FIELD  = "sourceField";
    public static final String TARGET_FIELD  = "targetField";

    // ── deleteNode 通用 ───────────────────────────────────
    public static final String VAL = "val";
}
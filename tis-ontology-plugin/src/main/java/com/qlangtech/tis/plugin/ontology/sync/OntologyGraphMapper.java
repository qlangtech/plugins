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

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr;
import com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT;
import com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DataSourceBinding;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.impl.role.MeasureRole;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ontology.sync.CypherParams.*;

/**
 * 本体实体 → Cypher 参数 Map 映射。供 {@link OntologyNeo4jSyncService} 调用。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class OntologyGraphMapper {

    private final OntologyEmbeddingService embeddingService;

    public OntologyGraphMapper(OntologyEmbeddingService embeddingService) {
        this.embeddingService = embeddingService;
    }

    /**
     * ObjectType 节点参数。
     */
    public Map<String, Object> toObjectTypeParams(String domain, OntologyObjectType ot) {
        Map<String, Object> p = new HashMap<>();
        p.put(DOMAIN, domain);
        p.put(NAME, ot.getName());
        ObjectTypeBinding.ObjectTypeBindingInfo info = ot.getObjectTypeBindingInfo();
        p.put(ALIAS, nullSafe(info.description()));

        String boundDsName = "";
        String physicalTable = "";
        if (ot.getDataSourceBinding() instanceof DataSourceBinding dsb) {
            boundDsName = nullSafe(dsb.dbName);
            physicalTable = dsb.resolvePhysicalTableName(ot.getName());
        }
        p.put(BOUND_DS_NAME, boundDsName);
        p.put(PHYSICAL_TABLE, physicalTable);
        p.put(EMBEDDING, embeddingService.embedAsList(EmbeddingTextBuilder.forObjectType(ot)));
        return p;
    }

    /**
     * Property 节点参数。
     */
    public Map<String, Object> toPropertyParams(String domain, String otName, OntologyProperty prop, int ordinal) {
        Map<String, Object> p = new HashMap<>();
        p.put(DOMAIN, domain);
        p.put(OT_NAME, otName);
        p.put(NAME, prop.getName());
        p.put(TYPE, prop.getType());
        p.put(END_TYPE, prop.getTypeEnd());
        p.put(PK, Boolean.TRUE.equals(prop.isPk()));
        p.put(NULLABLE, Boolean.TRUE.equals(prop.isNullable()));
        p.put(DESCRIPTION, nullSafe(prop.getDescription()));
        p.put(ORDINAL, ordinal);

        String role = "Unknown";
        String agg = null;
        if (prop instanceof DefaultOntologyProperty dp) {
            role = dp.getSemanticRole().name();
            if (dp.roleType instanceof MeasureRole mr && mr.getAggregation() != null) {
                agg = mr.getAggregation().kind().name();
            }
        }
        p.put(ROLE, role);
        p.put(AGG, agg);
        p.put(EMBEDDING, embeddingService.embedAsList(EmbeddingTextBuilder.forProperty(otName, prop)));
        return p;
    }

    /**
     * SharedProperty 节点参数。
     */
    public Map<String, Object> toSharedPropertyParams(String domain, OntologySharedProperty sp) {
        Map<String, Object> p = new HashMap<>();
        p.put(DOMAIN, domain);
        p.put(NAME, sp.name);
        p.put(ALIAS, nullSafe(sp.alias));
        p.put(DESCRIPTION, nullSafe(sp.description));
        p.put(TYPE, sp.getOntologyType().getLiteria());
        p.put(EMBEDDING, embeddingService.embedAsList(EmbeddingTextBuilder.forSharedProperty(sp)));
        return p;
    }

    /**
     * ValueType 节点参数。ValueType 不做 embedding。
     */
    public Map<String, Object> toValueTypeParams(String domain, OntologyValueType vt) {
        Map<String, Object> p = new HashMap<>();
        p.put(DOMAIN, domain);
        OntologyValueType.IMetadataOfValueType meta = vt.getMeta();
        p.put(NAME, meta.getName());
        p.put(DESCRIPTION, nullSafe(meta.getDescription()));
        p.put(ONTOLOGY_TYPE, meta.ontologyType().getLiteria());

        String constraintKind = "";
        String constraintParams = "";
        for (OneStepOfMultiSteps step : vt.getMultiStepsSavedItems()) {
            if (step instanceof ConstraintsOfValueType csv && csv.constraint != null) {
                constraintKind = csv.constraint.getClass().getSimpleName();
                constraintParams = JSON.toJSONString(csv.constraint);
                break;
            }
        }
        p.put(CONSTRAINT_KIND, constraintKind);
        p.put(CONSTRAINT_PARAMS, constraintParams);
        return p;
    }

    /**
     * Glossary 节点参数。
     */
    public Map<String, Object> toGlossaryParams(String domain, OntologyGlossary g) {
        Map<String, Object> p = new HashMap<>();
        p.put(DOMAIN, domain);
        p.put(TERM, g.term);
        List<String> syns = g.getSynonyms().stream()
                .map(s -> s.getEnumVal())
                .collect(Collectors.toList());
        p.put(SYNONYMS, syns);
        p.put(DESCRIPTION, nullSafe(g.description));

        String targetKind = "";
        String targetRef = "";
        String metricSql = null;
        if (g.target instanceof GlossaryTargetOT tot) {
            targetKind = "OT";
            targetRef = nullSafe(tot.objectType);
        } else if (g.target instanceof GlossaryTargetProperty tp) {
            targetKind = "Property";
            targetRef = tp.objectType + "." + tp.targetField;
        } else if (g.target instanceof GlossaryTargetMetricExpr me) {
            targetKind = "MetricExpr";
            metricSql = me.sql;
        }
        p.put(TARGET_KIND, targetKind);
        p.put(TARGET_REF, targetRef);
        p.put(METRIC_SQL, metricSql);
        p.put(EMBEDDING, embeddingService.embedAsList(EmbeddingTextBuilder.forGlossary(g)));
        return p;
    }

    private static String nullSafe(String s) {
        return StringUtils.trimToEmpty(s);
    }
}
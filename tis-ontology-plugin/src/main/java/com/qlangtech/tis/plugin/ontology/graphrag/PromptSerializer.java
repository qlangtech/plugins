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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 将子图序列化为 Markdown prompt 上下文，按 token 预算逐档剪枝。
 *
 * <p>剪枝策略（粗到细，对应设计文档 §4 Step 3）：
 * <ol>
 *   <li>按 OT 得分降序保留至预算耗尽</li>
 *   <li>每个 OT 内部按 Property 得分截断（始终保留 PK + 出现在 Linker 上的列）</li>
 *   <li>预算不足时丢弃 description / 取值示例</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
final class PromptSerializer {

    private static final int CHARS_PER_TOKEN = 4;

    private PromptSerializer() {
    }

    static String serialize(SubgraphSnapshot snap, RetrievalOptions opts) {
        int charBudget = opts.tokenBudget() * CHARS_PER_TOKEN;
        boolean withDescription = true;

        for (int pass = 0; pass < 2; pass++) {
            String md = render(snap, opts, withDescription);
            if (md.length() <= charBudget) return md;
            withDescription = false;
        }
        return renderCompact(snap, charBudget);
    }

    private static String render(SubgraphSnapshot snap, RetrievalOptions opts, boolean withDescription) {
        StringBuilder sb = new StringBuilder(2048);

        if (!snap.glossaries.isEmpty()) {
            sb.append("## Glossary\n");
            List<SubgraphSnapshot.GlossaryNode> sorted = new ArrayList<>(snap.glossaries);
            sorted.sort(Comparator.comparingDouble((SubgraphSnapshot.GlossaryNode g) -> g.score).reversed());
            for (SubgraphSnapshot.GlossaryNode g : sorted) {
                sb.append("- **").append(g.term).append("**");
                if (g.synonyms != null && !g.synonyms.isEmpty()) {
                    sb.append(" (").append(String.join(", ", g.synonyms)).append(")");
                }
                if ("MetricExpr".equals(g.targetKind) && StringUtils.isNotBlank(g.metricSql)) {
                    sb.append(" → `").append(g.metricSql).append("`");
                } else if (StringUtils.isNotBlank(g.targetRef)) {
                    sb.append(" → ").append(g.targetKind).append(":").append(g.targetRef);
                }
                if (withDescription && StringUtils.isNotBlank(g.description)) {
                    sb.append("  \n  ").append(g.description);
                }
                sb.append('\n');
            }
            sb.append('\n');
        }

        if (!snap.objectTypes.isEmpty()) {
            sb.append("## ObjectTypes\n");
            List<SubgraphSnapshot.ObjectTypeNode> ots = new ArrayList<>(snap.objectTypes.values());
            ots.sort(Comparator.comparingDouble((SubgraphSnapshot.ObjectTypeNode o) -> o.score).reversed());
            for (SubgraphSnapshot.ObjectTypeNode ot : ots) {
                sb.append("### ").append(ot.name);
                if (StringUtils.isNotBlank(ot.alias)) sb.append(" (").append(ot.alias).append(')');
                sb.append('\n');
                if (StringUtils.isNotBlank(ot.physicalTable)) {
                    sb.append("- physical: `").append(ot.physicalTable).append("`");
                    if (StringUtils.isNotBlank(ot.boundDsName)) {
                        sb.append(" @ ").append(ot.boundDsName);
                    }
                    sb.append('\n');
                }
                if (withDescription && StringUtils.isNotBlank(ot.description)) {
                    sb.append("- description: ").append(ot.description).append('\n');
                }
                if (!ot.properties.isEmpty()) {
                    sb.append("- columns:\n");
                    for (SubgraphSnapshot.PropertyNode p : ot.properties) {
                        appendProperty(sb, p, opts.includeValueExamples(), withDescription);
                    }
                }
                sb.append('\n');
            }
        }

        if (!snap.sharedProperties.isEmpty()) {
            sb.append("## SharedProperties\n");
            for (SubgraphSnapshot.SharedPropertyNode sp : snap.sharedProperties.values()) {
                sb.append("- **").append(sp.name).append("**");
                if (StringUtils.isNotBlank(sp.alias)) sb.append(" (").append(sp.alias).append(')');
                sb.append(" : ").append(sp.type);
                if (withDescription && StringUtils.isNotBlank(sp.description)) {
                    sb.append("  ").append(sp.description);
                }
                sb.append('\n');
            }
            sb.append('\n');
        }

        if (!snap.linkers.isEmpty()) {
            sb.append("## Linkers\n");
            for (LinkerInfo l : snap.linkers) {
                sb.append("- ").append(l.source()).append('.').append(l.sourceField())
                        .append(" -[").append(l.relType()).append('/').append(l.cardinality()).append("]-> ")
                        .append(l.target()).append('.').append(l.targetField()).append('\n');
            }
        }

        return sb.toString();
    }

    private static void appendProperty(StringBuilder sb, SubgraphSnapshot.PropertyNode p,
                                       boolean includeValueExamples, boolean withDescription) {
        sb.append("  - ").append(p.name).append(" : ").append(p.type);
        if (p.pk) sb.append(" PK");
        if (!p.nullable) sb.append(" NOT_NULL");
        if (p.role != null && !"Unknown".equals(p.role)) sb.append(" [").append(p.role).append(']');
        if (StringUtils.isNotBlank(p.agg)) sb.append(" agg=").append(p.agg);
        if (StringUtils.isNotBlank(p.physicalExpr)) {
            sb.append(" **physical=`").append(p.physicalExpr).append("`**");
        }
        if (StringUtils.isNotBlank(p.sharedPropRef)) sb.append(" → SP:").append(p.sharedPropRef);
        if (withDescription && StringUtils.isNotBlank(p.description)) {
            sb.append(" — ").append(p.description);
        }
        if (includeValueExamples && StringUtils.isNotBlank(p.constraintKind)
                && StringUtils.isNotBlank(p.constraintParams)) {
            sb.append(" values=").append(p.constraintParams);
        }
        sb.append('\n');
    }

    /**
     * 极致瘦身：仅给 OT 名 + 列名 + Linker，不带描述/取值。
     */
    private static String renderCompact(SubgraphSnapshot snap, int charBudget) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("## ObjectTypes (compact)\n");
        for (SubgraphSnapshot.ObjectTypeNode ot : snap.objectTypes.values()) {
            sb.append(ot.name).append(": ");
            for (int i = 0; i < ot.properties.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(ot.properties.get(i).name);
            }
            sb.append('\n');
            if (sb.length() > charBudget) break;
        }
        if (!snap.linkers.isEmpty() && sb.length() < charBudget) {
            sb.append("## Linkers\n");
            for (LinkerInfo l : snap.linkers) {
                sb.append(l.source()).append('.').append(l.sourceField())
                        .append(" -> ").append(l.target()).append('.').append(l.targetField()).append('\n');
                if (sb.length() > charBudget) break;
            }
        }
        return sb.toString();
    }
}

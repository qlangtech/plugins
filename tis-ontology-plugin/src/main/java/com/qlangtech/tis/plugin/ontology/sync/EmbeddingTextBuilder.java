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

import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.synonyms.SynonymsElement;

import java.util.List;
import java.util.stream.Collectors;

/**
 * embedding 文本拼装策略（对照设计文档 §3）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class EmbeddingTextBuilder {

    private static final int MAX_COL_NAMES = 5;

    /**
     * ObjectType embedding 文本：name + alias + description + 前N列名。
     */
    public static String forObjectType(OntologyObjectType ot) {
        StringBuilder sb = new StringBuilder();
        sb.append(ot.getName());
        String alias = ot.getObjectTypeBindingInfo().description();
        if (alias != null && !alias.isBlank()) sb.append(' ').append(alias);
        // description 来自 ObjectTypeProfile
        List<OntologyProperty> cols = ot.getCols();
        if (!cols.isEmpty()) {
            String colNames = cols.stream()
                    .limit(MAX_COL_NAMES)
                    .map(OntologyProperty::getName)
                    .collect(Collectors.joining(" "));
            sb.append(' ').append(colNames);
        }
        return sb.toString().trim();
    }

    /**
     * Property embedding 文本：otName.name + alias(description) + role。
     */
    public static String forProperty(String otName, OntologyProperty prop) {
        StringBuilder sb = new StringBuilder();
        sb.append(otName).append('.').append(prop.getName());
        if (prop.getDescription() != null && !prop.getDescription().isBlank()) {
            sb.append(' ').append(prop.getDescription());
        }
        if (prop instanceof DefaultOntologyProperty dp) {
            sb.append(' ').append(dp.getSemanticRole().name());
        }
        return sb.toString().trim();
    }

    /**
     * SharedProperty embedding 文本：name + alias + description。
     */
    public static String forSharedProperty(OntologySharedProperty sp) {
        StringBuilder sb = new StringBuilder();
        sb.append(sp.name);
        if (sp.alias != null && !sp.alias.isBlank()) sb.append(' ').append(sp.alias);
        if (sp.description != null && !sp.description.isBlank()) sb.append(' ').append(sp.description);
        return sb.toString().trim();
    }

    /**
     * Glossary embedding 文本：term + synonyms + description。
     */
    public static String forGlossary(OntologyGlossary g) {
        StringBuilder sb = new StringBuilder();
        sb.append(g.term);
        List<SynonymsElement> synonyms = g.getSynonyms();
        if (!synonyms.isEmpty()) {
            String syns = synonyms.stream()
                    .map(SynonymsElement::getEnumVal)
                    .collect(Collectors.joining(" "));
            sb.append(' ').append(syns);
        }
        if (g.description != null && !g.description.isBlank()) sb.append(' ').append(g.description);
        return sb.toString().trim();
    }
}
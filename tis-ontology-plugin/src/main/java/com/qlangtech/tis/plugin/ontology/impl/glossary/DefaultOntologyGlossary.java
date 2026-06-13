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
package com.qlangtech.tis.plugin.ontology.impl.glossary;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ds.BasicMultiSelectSingleValElementCreatorFactory;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.plugin.ontology.sync.OntologySyncQueue;
import com.qlangtech.tis.util.IPluginContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class DefaultOntologyGlossary extends OntologyGlossary implements IPluginStore.AfterPluginSaved {

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        String domain = OntologyPluginMeta.createPluginMeta(pluginContext.getContext()).getDomain();
        final DefaultOntologyGlossary self = this;
        OntologySyncQueue.enqueue(new OntologySyncQueue.OntologySyncTask(pluginContext) {
            @Override
            protected void sync() {
                OntologyNeo4jSyncService.getInstance().syncGlossary(domain, self);
            }
        });
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = Lists.newArrayList();
        literia.add(new UDFDesc("Term", this.term));
        literia.add(new UDFDesc("Description", this.description));
        // Synonyms: join all synonym values as a comma-separated string
        List<String> synonymVals = this.getSynonyms().stream()
                .map(BasicMultiSelectSingleValElementCreatorFactory.OneOfMultiElement::getEnumVal)
                .collect(java.util.stream.Collectors.toList());
        literia.add(new UDFDesc("Synonyms", String.join(", ", synonymVals)));
        // Target: use getTargetLiteral() which provides a meaningful description of the target
        literia.add(new UDFDesc("Target", this.target.getTargetLiteral()));
        return literia;
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc {
        public DefaultDesc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.OntologyGlossary;
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.Glossary;
        }

        @Override
        public String getDisplayName() {
            return "Glossary";
        }

        @Override
        public String shortComment() {
            return "定义业务术语及同义词，将自然语言映射到本体元素";
        }
    }
}
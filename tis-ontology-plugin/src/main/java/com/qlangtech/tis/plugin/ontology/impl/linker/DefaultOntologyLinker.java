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
package com.qlangtech.tis.plugin.ontology.impl.linker;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.aiagent.llm.ITISJsonSchema;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.plugin.ontology.sync.OntologySyncQueue;
import com.qlangtech.tis.util.DescriptorsJSONForAIPrompt;
import com.qlangtech.tis.util.DescriptorsMeta;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 * @see RelationshipTypeSetter
 * @see LinkResources
 * @see RelationshipTypeObjectTypeForeignKeys
 * @see RelationshipTypeJoinTableDataset
 * @see RelationshipTypeBackingObjectType
 */
public class DefaultOntologyLinker extends OntologyLinker implements IPluginStore.AfterPluginSaved {

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        String domain = OntologyPluginMeta.createPluginMeta(pluginContext.getContext()).getDomain();
        final DefaultOntologyLinker self = this;
        OntologySyncQueue.enqueue(() -> OntologyNeo4jSyncService.getInstance().syncLinker(domain, self));
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc implements MultiStepsSupportHostDescriptor<OntologyLinker> {
        public DefaultDesc() {
            super();
        }

        @Override
        public List<List<ITISJsonSchema>> generateMultiStepsSchemaForAIPrompt() {
            List<List<ITISJsonSchema>> result = Lists.newArrayList();
            for (RelationshipType relationType : RelationshipType.values()) {
                List<ITISJsonSchema> oneOfSteps = Lists.newArrayList();
                DescriptorsJSONForAIPrompt<?> inner = new DescriptorsJSONForAIPrompt<>(
                        Collections.singletonList(new RelationshipTypeSetter.Desc()), false,
                        (builder, descriptor) -> {},
                        (attr, addedProp) -> {
                            if (StringUtils.equals(attr.getFieldKey(), RelationshipTypeSetter.KEY_RELATIONSHIP_TYPE)) {
                                addedProp.setConst(relationType.getToken(), relationType.getDescription());
                                return true;
                            }
                            return false;
                        });
                DescriptorsMeta innerMeta = inner.getDescriptorsJSON();
                oneOfSteps.add(innerMeta.getFirstPluginJsonSchema());
                inner = new DescriptorsJSONForAIPrompt<>(
                        Collections.singletonList(LinkerUtils.getObjectTypeLinkerDescriptor(relationType)), false);
                innerMeta = inner.getDescriptorsJSON();
                oneOfSteps.add(innerMeta.getFirstPluginJsonSchema());
                result.add(oneOfSteps);
            }
            return result;
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.Linker;
        }

        @Override
        public EndType getEndType() {
            return EndType.OntologyLink;
        }

        @Override
        public String getDisplayName() {
            return "Link Type";
        }

        @Override
        public Class<OntologyLinker> getHostClass() {
            return OntologyLinker.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(new RelationshipTypeSetter.Desc(), new RelationshipTypeObjectTypeForeignKeys.DefDesc());
        }

        @Override
        public void appendExternalProps(JSONObject multiStepsCfg) {
        }

        @Override
        public String shortComment() {
            return "定义本体对象类型间的关联关系（Link Type）";
        }
    }
}
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
package com.qlangtech.tis.plugin.ontology.impl.sharedproperty;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.sync.OntologyNeo4jSyncService;
import com.qlangtech.tis.plugin.ontology.sync.OntologySyncQueue;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class DefaultOntologySharedProperty extends OntologySharedProperty implements IPluginStore.AfterPluginSaved {

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        String domain = OntologyPluginMeta.createPluginMeta(pluginContext.getContext()).getDomain();
        final DefaultOntologySharedProperty self = this;
        OntologySyncQueue.enqueue(() -> OntologyNeo4jSyncService.getInstance().syncSharedProperty(domain, self));
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc {
        public DefaultDesc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.Shared;
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.SharedProperty;
        }

        @Override
        public String getDisplayName() {
            return "Shared Property";
        }

        @Override
        public String shortComment() {
            return "定义可跨对象类型复用的共享属性";
        }
    }
}
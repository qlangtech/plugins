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

package com.qlangtech.tis.plugin.ontology.impl.domain;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.IDescribableManipulate;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyDomainManipulate;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class DefaultOntologyDomain extends OntologyDomain implements IPluginStore.AfterPluginSaved {

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {

    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<OntologyDomain> implements DescriptorUseableShortComment, IDescribableManipulate<OntologyDomainManipulate> {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return StoreResourceType.Ontology.pluginDescName;
        }

        @Override
        public String shortComment() {
            return "定义本体顶层命名空间（域），用于隔离不同业务场景";
        }

        @Override
        public Class<OntologyDomainManipulate> getManipulateExtendPoint() {
            return OntologyDomainManipulate.class;
        }

        @Override
        public final Optional<IPluginStore<OntologyDomainManipulate>> getManipulateStore() {
            OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
            KeyedPluginStore.Key<OntologyDomainManipulate> manipulateStoreKey
                    = OntologyDomain.getStoreKey(pluginMeta.getDomain(), this.getManipulateExtendPoint());
            return Optional.of(TIS.getPluginStore(manipulateStoreKey));
        }
    }
}



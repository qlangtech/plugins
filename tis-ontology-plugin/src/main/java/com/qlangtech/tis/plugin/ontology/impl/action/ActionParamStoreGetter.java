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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.storegetter.BaiscAssistStoreGetter;

import java.io.File;
import java.util.Optional;

/**
 * Action 参数池（{@link ActionParameterConfig}）专用的 store getter。
 *
 * <p><b>为什么不能直接用 {@code Ontology.OntologyEnum.Action.storeKeyGetter}</b>：
 * 它的 {@code getPluginType()} 沿用父类实现，返回硬编码的 {@code ONTOLOGY.extensionPoint}
 * （即 {@code Ontology.class}），而 {@code PluginStore.setPlugins()} 会拿它做
 * {@code pluginClass.isAssignableFrom(instance.getClass())} 校验 ——
 * {@link ActionParameterConfig} 不是 {@code Ontology} 的子类，落盘时会直接抛
 * 「plugin must be type of …Ontology」。读侧同理：{@code PluginStore} 按同一个
 * pluginClass 反序列化。核心工程无需改动，{@code BaiscAssistStoreGetter} 已经
 * 把该值收敛成 {@code getPluginType()} 虚方法，本类只是把它指回元素类型。
 *
 * <p>同一手法的既有先例：{@code WorkshopVariable.WorkshopVariableStoreGetter}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParameterConfig
 */
public class ActionParamStoreGetter extends BaiscAssistStoreGetter<ActionParameterConfig> {

    @Override
    protected Class<ActionParameterConfig> getPluginType() {
        return ActionParameterConfig.class;
    }

    @Override
    public File getAssistRootDir(String ontologyName, Optional<String> subModule) {
        // 参数池目录仍挂在 action-type 下：<ontologyDomainDir>/action-type/<subModule>，
        // 其中 subModule 形如 params/<parameterId>。目录约定与 Action 系保持一致，
        // 只是插件类型不同，所以这里直接委托回去而不另立一套路径。
        return Ontology.OntologyEnum.Action.storeKeyGetter.getAssistRootDir(ontologyName, subModule);
    }

    @Override
    public IPluginStore<ActionParameterConfig> getPluginStore(OntologyPluginMeta pluginMeta, Optional<String> subModule) {
        // 读侧同样要拿到真实 store：父类在 isUpdate()/shallPersistence() 都为假时返回
        // noSaveStore，其 getPlugin() 恒为 null，参数池会被静默读空。
        return super.getPluginStore(pluginMeta.setPersistence(), subModule);
    }
}

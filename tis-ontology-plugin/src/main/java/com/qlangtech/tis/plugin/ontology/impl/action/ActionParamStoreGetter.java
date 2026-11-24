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
 * Action 参数池（{@link ActionParameters}）专用的 store getter。
 *
 * <p><b>store 的插件类型是宿主 {@link ActionParameters} 而非元素 {@link ActionParameterConfig}</b>：
 * 参数池是「一个 Action 一份」的宿主级持久化（文件 {@code params/<actionName>}，见
 * {@link ActionParameters#getActionParamsStore}），写入方传的是
 * {@code new Descriptor.ParseDescribable(this)}（宿主实例），读取方
 * {@code ActionParameters#loadParameters} 把 {@code getPlugin()} 强转回宿主
 * （{@code getPlugin()} 在 store 里多于一个插件时直接抛异常，天然单实例语义）。
 * 曾把它声明成元素类型 {@code ActionParameterConfig}，结果
 * {@code PluginStore.setPlugins()} 的 {@code pluginClass.isAssignableFrom(instance.getClass())}
 * 校验必炸：「plugin must be type of …ActionParameterConfig, but now is …ActionParameters」。
 * 宿主级落盘还保证 {@code submitButton}/{@code successMessage} 两个宿主字段随参数池一起持久化 ——
 * 若只存元素列表它们会丢。
 *
 * <p><b>为什么不能直接用 {@code Ontology.OntologyEnum.Action.storeKeyGetter}</b>：
 * 它的 {@code getPluginType()} 沿用父类实现，返回硬编码的 {@code ONTOLOGY.extensionPoint}
 * （即 {@code Ontology.class}），而 {@code ActionParameters} 不在 {@code Ontology} 的继承链上。
 * 核心工程无需改动，{@code BaiscAssistStoreGetter} 已经把该值收敛成 {@code getPluginType()} 虚方法。
 *
 * <p>同一手法的既有先例：{@code WorkshopVariable.WorkshopVariableStoreGetter}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParameters
 */
public class ActionParamStoreGetter extends BaiscAssistStoreGetter<ActionParameters> {

    @Override
    protected Class<ActionParameters> getPluginType() {
        return ActionParameters.class;
    }

    @Override
    public File getAssistRootDir(String ontologyName, Optional<String> subModule) {
        // 参数池目录仍挂在 action-type 下：<ontologyDomainDir>/action-type/<subModule>，
        // 其中 subModule 形如 params/<actionName>。目录约定与 Action 系保持一致，
        // 只是插件类型不同，所以这里直接委托回去而不另立一套路径。
        return Ontology.OntologyEnum.Action.storeKeyGetter.getAssistRootDir(ontologyName, subModule);
    }

    @Override
    public IPluginStore<ActionParameters> getPluginStore(OntologyPluginMeta pluginMeta, Optional<String> subModule) {
        // 读侧同样要拿到真实 store：父类在 isUpdate()/shallPersistence() 都为假时返回
        // noSaveStore，其 getPlugin() 恒为 null，参数池会被静默读空。
        return super.getPluginStore(pluginMeta.setPersistence(), subModule);
    }
}

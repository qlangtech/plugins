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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameters;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.DefaultOntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 从 Action 参数池中取值。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/12
 */
public class FromParameterValueSource extends ValueSourceConfig {
    public FromParameterValueSource(String parameterRef) {
        this.parameterRef = parameterRef;
    }

    public FromParameterValueSource() {
    }

    /**
     * 参数名。
     *
     * <p>2026/11/24 起提供下拉候选（见 {@link DftDescriptor#loadParameterRefOptions()}）：
     * 编辑态候选取自本 Action 已落盘的参数池；创建向导早期参数池尚未落盘时回退到
     * 「规则推导的 canonical 参数名」。候选仍可能为空（规则也未持久化的首配窗口），
     * 「参数是否存在」的最终校验由执行期覆盖检查兜底，本字段上的 {@code require} 兜住空值。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String parameterRef;

    @Override
    public Object resolveValue(Map<String, Object> parameters) {
        return parameters.get(parameterRef);
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor {

        public DftDescriptor() {
            super();
            // SELECTABLE 必须在 descriptor 上登记候选来源，否则 DescriptorsJSON#getSelectOptions
            // 直接抛异常、整张表单打不开。与 ModifyObjectRule 同款兜底原则：候选由 supplier 现场加载，
            // 加载不到（创建向导最早期 / 上下文缺失）回退空列表，绝不抛。
            this.registerSelectOptions("parameterRef", this::loadParameterRefOptions);
        }

        @Override
        public String getDisplayName() {
            return "Action 参数";
        }

        /**
         * {@code parameterRef} 候选 = 本 Action 参数池的全部参数（label 用 displayName 口径）。
         *
         * <p>创建向导早期参数池尚未落盘时，规则（第 2 步已保存）推导的 canonical 参数名兜底——
         * 多对象类型 Action 下映射行该引用的正是带命名空间前缀的 canonical 参数
         * （见 {@code ObjectActionType#resolveCanonicalParams}）；连规则也取不到的首配窗口回退空候选。
         */
        List<Option> loadParameterRefOptions() {
            try {
                OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
                ActionParameters paramPool = ActionParameters.loadParameters(pluginMeta);
                List<ActionParameterConfig> parameters = paramPool.getParameters();
                if (!parameters.isEmpty()) {
                    return poolToOptions(parameters);
                }
                OntologyAction action = Ontology.loadActionDetail(pluginMeta.getDomain(), pluginMeta.getPluginIdVal());
                if (action instanceof DefaultOntologyAction ontologyAction) {
                    List<String> objectTypeRefs = ObjectActionType.objectTypeReferences(ontologyAction.getActionTypes());
                    List<ObjectActionType> objectRules = ontologyAction.getActionTypes().stream()
                            .filter((t) -> t instanceof ObjectActionType)
                            .map((t) -> (ObjectActionType) t).toList();
                    return canonicalToOptions(ObjectActionType.resolveCanonicalParams(objectRules, objectTypeRefs)
                            .values());
                }
            } catch (RuntimeException e) {
                // 半配置状态：空候选兜底，绝不阻断表单渲染
            }
            return Collections.emptyList();
        }

        /**
         * 参数池 → 候选（包级静态以便单测）：label 与 val 恒为 parameterId
         * （{@link ActionParameterConfig#identityValue()}）。
         *
         * <p>刻意不用 displayName 做 label：映射行落盘的是 parameterRef 裸名，而参数池表格、
         * C1 共享字段候选标注（「已由 &lt;canonical 参数名&gt; 提供」）、C4 报错
         * （{@code target ← parameterRef}）说的全是 parameterId——下拉若显示 displayName，
         * 用户选中后在映射表里看到的是另一个字符串，难以核对；且 displayName 可能截断或重名，
         * 反而更迷惑。
         */
        static List<Option> poolToOptions(List<ActionParameterConfig> parameters) {
            return parameters.stream()
                    .map((param) -> new Option(param.identityValue(), param.identityValue()))
                    .collect(Collectors.toList());
        }

        /**
         * canonical 参数名 → 候选（包级静态以便单测）：创建向导早期参数池未落盘的兜底通道，
         * label 与 val 同为带命名空间前缀的参数名（此时尚无 displayName 可用）。
         */
        static List<Option> canonicalToOptions(Iterable<String> canonicalParamIds) {
            List<Option> options = new java.util.ArrayList<>();
            for (String paramId : canonicalParamIds) {
                options.add(new Option(paramId, paramId));
            }
            return options;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals vals) {
            FromParameterValueSource source = vals.newInstance();
            if (StringUtils.isEmpty(source.parameterRef)) {
                msgHandler.addErrorMessage(context, "请填写参数名");
                return false;
            }
            return super.validateAll(msgHandler, context, vals);
        }
    }
}

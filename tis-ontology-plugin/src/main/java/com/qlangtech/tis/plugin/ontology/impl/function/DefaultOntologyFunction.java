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

package com.qlangtech.tis.plugin.ontology.impl.function;

import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyFunction;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;

/**
 * 默认的 Ontology Function 实现
 *
 * 定义计算和查询逻辑，包括：
 * - Query Functions（只读查询）
 * - Ontology Edit Functions（写入操作）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 * @see FunctionMetadata
 * @see FunctionImplementation
 */
public class DefaultOntologyFunction extends OntologyFunction {

    @Override
    public String identityValue() {
        // 从第一步获取名称作为标识
        return getName();
    }

    @Override
    public void manipuldateProcess(IPluginContext pluginContext,
                                   com.qlangtech.tis.util.UploadPluginMeta pluginMeta,
                                   java.util.Optional<com.alibaba.citrus.turbine.Context> context) {
        // 可以在这里添加自定义持久化逻辑
        // 例如：验证函数代码语法、编译检查等
    }

    /**
     * 获取第一步（Metadata）
     */
    private FunctionMetadata getMetadata() {
        return (FunctionMetadata) getMultiStepsSavedItems()[0];
    }

    /**
     * 获取第二步（Implementation）
     */
    private FunctionImplementation getImplementation() {
        return (FunctionImplementation) getMultiStepsSavedItems()[1];
    }

    @Override
    public String getName() {
        return getMetadata().name;
    }

    @Override
    public String getDisplayName() {
        return getMetadata().displayName;
    }

    @Override
    public String getDescription() {
        return getMetadata().description;
    }

    /**
     * 获取编程语言
     */
    public String getLanguage() {
        return getMetadata().language;
    }

    /**
     * 获取函数类型
     */
    public String getFunctionType() {
        return getMetadata().functionType;
    }

    /**
     * 获取输入参数配置（JSON）
     */
    public String getInputParameters() {
        return getImplementation().inputParameters;
    }

    /**
     * 获取返回类型
     */
    public String getReturnType() {
        return getImplementation().returnType;
    }

    /**
     * 获取函数实现代码（Groovy）
     */
    public String getImplementationCode() {
        return getImplementation().implementation;
    }

    /**
     * 获取测试用例（JSON，可选）
     */
    public String getTestCases() {
        return getImplementation().testCases;
    }

    @Override
    public java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> getLiteria() {
        java.util.List<com.qlangtech.tis.plugin.datax.transformer.UDFDesc> literia =
            com.google.common.collect.Lists.newArrayList();

        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Name", getName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Display Name", getDisplayName()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Language", getLanguage()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Function Type", getFunctionType()));
        literia.add(new com.qlangtech.tis.plugin.datax.transformer.UDFDesc("Return Type", getReturnType()));

        return literia;
    }

    @TISExtension
    public static class DefaultDesc extends Ontology.BasicDesc
            implements MultiStepsSupportHostDescriptor<OntologyFunction> {

        @Override
        public Class<OntologyFunction> getHostClass() {
            return OntologyFunction.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return List.of(
                new FunctionMetadata.Desc(),
                new FunctionImplementation.Desc()
            );
        }

        @Override
        public void appendExternalProps(com.alibaba.fastjson.JSONObject multiStepsCfg) {
            // 可以在这里添加额外的配置信息到前端
        }

        @Override
        public OntologyEnum getOntologyType() {
            return OntologyEnum.Function;
        }

        @Override
        public String getDisplayName() {
            return "Function";
        }

        @Override
        public String shortComment() {
            return "定义本体函数，用于计算和查询逻辑";
        }
    }
}

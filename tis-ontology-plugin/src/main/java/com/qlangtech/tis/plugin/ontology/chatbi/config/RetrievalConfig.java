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
package com.qlangtech.tis.plugin.ontology.chatbi.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalOptions;

/**
 * GraphRAG 检索配置
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class RetrievalConfig implements Describable<RetrievalConfig> {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer topKSeeds;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxHops;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer tokenBudget;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean includeValueExamples;

    /**
     * 转换为 RetrievalOptions
     */
    public RetrievalOptions toRetrievalOptions() {
        return new RetrievalOptions(
                topKSeeds != null ? topKSeeds : 5,
                maxHops != null ? maxHops : 2,
                tokenBudget != null ? tokenBudget : 3000,
                includeValueExamples != null ? includeValueExamples : false
        );
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<RetrievalConfig> implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Retrieval Config";
        }

        @Override
        public String shortComment() {
            return "Neo4j GraphRAG 检索配置";
        }
    }
}

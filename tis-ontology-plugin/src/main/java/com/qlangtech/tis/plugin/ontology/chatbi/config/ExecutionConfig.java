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

import java.time.Duration;

/**
 * SQL 执行配置
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class ExecutionConfig implements Describable<ExecutionConfig> {

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean executeQuery;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxResultRows;

    @FormField(ordinal = 3, type = FormFieldType.DURATION_OF_SECOND, validate = {Validator.require, Validator.integer})
    public Duration queryTimeout;

    public boolean isExecuteQuery() {
        return executeQuery != null ? executeQuery : true;
    }

    public int getMaxResultRows() {
        return maxResultRows != null ? maxResultRows : 200;
    }

    public int getQueryTimeout() {
        return (int) queryTimeout.getSeconds();
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ExecutionConfig> implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Execution Config";
        }

        @Override
        public String shortComment() {
            return "SQL 执行配置";
        }
    }
}

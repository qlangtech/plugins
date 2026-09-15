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

import java.util.HashMap;
import java.util.Map;

/**
 * Rule 执行上下文
 *
 * 包含执行 Rule 所需的所有信息：
 * - 参数值
 * - 目标对象
 * - 用户信息
 * - 临时数据存储
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class RuleExecutionContext {

    /**
     * 本体域名称
     */
    private final String ontologyDomain;

    /**
     * 目标对象类型
     */
    private final String targetObjectType;

    /**
     * Action 参数值
     */
    private final Map<String, Object> parameters;

    /**
     * 执行用户
     */
    private final String userId;

    /**
     * 临时数据存储（供规则间传递数据）
     */
    private final Map<String, Object> contextData;

    public RuleExecutionContext(String ontologyDomain, String targetObjectType,
                               Map<String, Object> parameters, String userId) {
        this.ontologyDomain = ontologyDomain;
        this.targetObjectType = targetObjectType;
        this.parameters = parameters != null ? parameters : new HashMap<>();
        this.userId = userId;
        this.contextData = new HashMap<>();
    }

    public String getOntologyDomain() {
        return ontologyDomain;
    }

    public String getTargetObjectType() {
        return targetObjectType;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public Object getParameter(String name) {
        return parameters.get(name);
    }

    public <T> T getParameter(String name, Class<T> type) {
        Object value = parameters.get(name);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    public String getUserId() {
        return userId;
    }

    /**
     * 设置上下文数据（供规则间传递数据）
     */
    public void setContextData(String key, Object value) {
        contextData.put(key, value);
    }

    /**
     * 获取上下文数据
     */
    public Object getContextData(String key) {
        return contextData.get(key);
    }

    /**
     * 获取上下文数据（带类型转换）
     */
    public <T> T getContextData(String key, Class<T> type) {
        Object value = contextData.get(key);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    /**
     * 判断上下文数据是否存在
     */
    public boolean hasContextData(String key) {
        return contextData.containsKey(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder 模式
     */
    public static class Builder {
        private String ontologyDomain;
        private String targetObjectType;
        private Map<String, Object> parameters = new HashMap<>();
        private String userId;

        public Builder ontologyDomain(String ontologyDomain) {
            this.ontologyDomain = ontologyDomain;
            return this;
        }

        public Builder targetObjectType(String targetObjectType) {
            this.targetObjectType = targetObjectType;
            return this;
        }

        public Builder parameters(Map<String, Object> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder parameter(String name, Object value) {
            this.parameters.put(name, value);
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public RuleExecutionContext build() {
            return new RuleExecutionContext(ontologyDomain, targetObjectType, parameters, userId);
        }
    }
}
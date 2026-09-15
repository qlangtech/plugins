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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rule 执行结果
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class RuleExecutionResult {

    /**
     * 执行是否成功
     */
    private final boolean success;

    /**
     * 错误信息（失败时）
     */
    private final String errorMessage;

    /**
     * 受影响的对象 RID 列表
     */
    private final List<String> affectedObjectRids;

    /**
     * 执行结果数据（可选，供后续规则使用）
     */
    private final Map<String, Object> resultData;

    /**
     * 执行耗时（毫秒）
     */
    private final long executionTimeMs;

    private RuleExecutionResult(boolean success, String errorMessage,
                               List<String> affectedObjectRids,
                               Map<String, Object> resultData,
                               long executionTimeMs) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.affectedObjectRids = affectedObjectRids != null ? affectedObjectRids : new ArrayList<>();
        this.resultData = resultData != null ? resultData : new HashMap<>();
        this.executionTimeMs = executionTimeMs;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isFailed() {
        return !success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public List<String> getAffectedObjectRids() {
        return affectedObjectRids;
    }

    public Map<String, Object> getResultData() {
        return resultData;
    }

    public Object getResultData(String key) {
        return resultData.get(key);
    }

    public <T> T getResultData(String key, Class<T> type) {
        Object value = resultData.get(key);
        if (value == null) {
            return null;
        }
        return type.cast(value);
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    /**
     * 创建成功结果
     */
    public static RuleExecutionResult success() {
        return new Builder().success(true).build();
    }

    /**
     * 创建成功结果（带受影响的对象）
     */
    public static RuleExecutionResult success(List<String> affectedObjectRids) {
        return new Builder()
            .success(true)
            .affectedObjectRids(affectedObjectRids)
            .build();
    }

    /**
     * 创建失败结果
     */
    public static RuleExecutionResult failure(String errorMessage) {
        return new Builder()
            .success(false)
            .errorMessage(errorMessage)
            .build();
    }

    /**
     * 创建失败结果（带异常）
     */
    public static RuleExecutionResult failure(Throwable throwable) {
        return new Builder()
            .success(false)
            .errorMessage(throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getName())
            .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder 模式
     */
    public static class Builder {
        private boolean success = true;
        private String errorMessage;
        private List<String> affectedObjectRids = new ArrayList<>();
        private Map<String, Object> resultData = new HashMap<>();
        private long executionTimeMs = 0;

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder affectedObjectRids(List<String> affectedObjectRids) {
            this.affectedObjectRids = affectedObjectRids;
            return this;
        }

        public Builder addAffectedObjectRid(String rid) {
            this.affectedObjectRids.add(rid);
            return this;
        }

        public Builder resultData(Map<String, Object> resultData) {
            this.resultData = resultData;
            return this;
        }

        public Builder putResultData(String key, Object value) {
            this.resultData.put(key, value);
            return this;
        }

        public Builder executionTimeMs(long executionTimeMs) {
            this.executionTimeMs = executionTimeMs;
            return this;
        }

        public RuleExecutionResult build() {
            return new RuleExecutionResult(success, errorMessage, affectedObjectRids, resultData, executionTimeMs);
        }
    }
}
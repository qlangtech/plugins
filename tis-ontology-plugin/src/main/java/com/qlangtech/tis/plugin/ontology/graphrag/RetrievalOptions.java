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
package com.qlangtech.tis.plugin.ontology.graphrag;

/**
 * GraphRAG 检索可调参数。
 *
 * @param topKSeeds            每个向量索引各取的入口节点数
 * @param maxHops              沿 LINKED_TO 关系扩展的最大跳数
 * @param tokenBudget          序列化 prompt 的 token 上限（按 4 chars ≈ 1 token 估算）
 * @param includeValueExamples 是否在 prompt 中带上 Enum 等取值示例
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
public record RetrievalOptions(
        int topKSeeds,
        int maxHops,
        int tokenBudget,
        boolean includeValueExamples
) {

    public static RetrievalOptions defaults() {
        return new RetrievalOptions(5, 2, 3000, false);
    }
}

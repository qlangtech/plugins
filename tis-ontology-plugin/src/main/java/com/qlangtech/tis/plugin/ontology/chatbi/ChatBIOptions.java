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
package com.qlangtech.tis.plugin.ontology.chatbi;

import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalOptions;

/**
 * ChatBI 查询选项。
 *
 * @param retrievalOptions GraphRAG 检索选项
 * @param executeQuery     是否执行生成的 SQL（false 则只生成不执行）
 * @param enableExplain    是否启用 EXPLAIN 动态校验
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public record ChatBIOptions(
        RetrievalOptions retrievalOptions,
        boolean executeQuery,
        boolean enableExplain
) {
    public static ChatBIOptions defaults() {
        return new ChatBIOptions(RetrievalOptions.defaults(), true, false);
    }

    public static ChatBIOptions withRetrieval(RetrievalOptions retrievalOptions) {
        return new ChatBIOptions(retrievalOptions, true, false);
    }
}

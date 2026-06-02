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

import java.util.List;

/**
 * GraphRAG 检索结果。
 *
 * @param promptContext  拼好的 Markdown prompt 上下文，可直接喂给 LLM
 * @param objectTypes    命中的 OT 名（用于 04 阶段 SQL 校验白名单）
 * @param linkers        命中的 LINKED_TO 关系
 * @param glossaryTerms  命中的术语
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
public record RetrievalResult(
        String promptContext,
        List<String> objectTypes,
        List<LinkerInfo> linkers,
        List<String> glossaryTerms
) {
}

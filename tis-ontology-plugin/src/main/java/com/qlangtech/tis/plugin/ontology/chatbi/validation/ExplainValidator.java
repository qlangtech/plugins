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
package com.qlangtech.tis.plugin.ontology.chatbi.validation;

import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;

/**
 * EXPLAIN 校验器（§5.2 T4，可选）。
 * <p>
 * 投递 EXPLAIN 到 Doris，捕获语义错误（如类型不匹配、聚合错误等）。
 * <p>
 * **一期暂不实现**：需要 Doris 连接池、超时控制（5s）、错误解析。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class ExplainValidator implements SqlValidator {

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        // TODO: 一期暂不实现 EXPLAIN 校验
        // 实现时需要：
        // 1. 从 DataSourceFactory 获取 Doris 连接
        // 2. 执行 EXPLAIN <sql>，设置超时 5s
        // 3. 解析 EXPLAIN 结果，捕获错误
        // 4. 返回 ValidationResult.fail(errorMsg)

        // 暂时直接通过
        return ValidationResult.ok();
    }
}

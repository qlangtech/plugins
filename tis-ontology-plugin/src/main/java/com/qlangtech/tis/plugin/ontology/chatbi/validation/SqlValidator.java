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
 * SQL 校验器接口。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public interface SqlValidator {

    /**
     * 校验 SQL。
     *
     * @param sql      待校验的 SQL
     * @param context  GraphRAG 检索上下文（提供表名/列名/关系白名单）
     * @return 校验结果
     */
    ValidationResult validate(String sql, RetrievalResult context);
}

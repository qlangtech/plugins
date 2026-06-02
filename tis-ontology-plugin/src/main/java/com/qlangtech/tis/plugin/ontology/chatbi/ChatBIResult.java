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

import java.util.List;

/**
 * ChatBI 查询结果。
 *
 * @param sql   最终生成的 SQL，失败为 null
 * @param data  执行结果（可选），null 表示未执行
 * @param trace 全过程日志（检索、prompt、LLM、校验、执行等步骤）
 * @param error 失败原因，成功为 null
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public record ChatBIResult(
        String sql,
        QueryResult data,
        List<TraceStep> trace,
        String error
) {
    public static ChatBIResult success(String sql, QueryResult data, List<TraceStep> trace) {
        return new ChatBIResult(sql, data, trace, null);
    }

    public static ChatBIResult fail(String error, List<TraceStep> trace) {
        return new ChatBIResult(null, null, trace, error);
    }

    public boolean isSuccess() {
        return error == null;
    }
}

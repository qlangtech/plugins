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

import java.util.List;

/**
 * SQL 校验结果。
 *
 * @param valid  是否通过校验
 * @param reason 失败原因（valid=false 时填充）
 * @param issues 详细问题列表（可选）
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public record ValidationResult(
        boolean valid,
        String reason,
        List<String> issues
) {
    public static ValidationResult ok() {
        return new ValidationResult(true, null, List.of());
    }

    public static ValidationResult fail(String reason) {
        return new ValidationResult(false, reason, List.of());
    }

    public static ValidationResult fail(String reason, List<String> issues) {
        return new ValidationResult(false, reason, issues);
    }
}

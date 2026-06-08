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

import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * SQL 校验结果。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class ValidationResult {

    private final boolean valid;
    private final String reason;
    private final List<String> issues;
    private final Exception exception;

    public ValidationResult(boolean valid, String reason, Exception exception, List<String> issues) {
        this.valid = valid;
        this.reason = reason;
        this.issues = issues;
        this.exception = exception;
    }

    public ValidationResult(boolean valid, String reason, List<String> issues) {
        this(valid, reason, (valid ? null : new Exception(reason)), issues);
    }

    public boolean valid() {
        return valid;
    }

    public String reason() {
        return reason;
    }

    public String reasonAndIssue() {
        StringBuffer buffer = new StringBuffer(this.reason());
        if (CollectionUtils.isNotEmpty(this.issues)) {
            buffer.append(",issue:").append(String.join(",", this.issues));
        }
        return buffer.toString();
    }

    public List<String> issues() {
        return issues;
    }

    public Exception exception() {
        return exception;
    }

    public static ValidationResult ok() {
        return new ValidationResult(true, null, List.of());
    }

    public static ValidationResult fail(String reason) {
        return new ValidationResult(false, reason, List.of());
    }

    public static ValidationResult fail(String reason, Exception epx) {
        return new ValidationResult(false, reason, epx, List.of());
    }

    public static ValidationResult fail(String reason, List<String> issues) {
        return new ValidationResult(false, reason, issues);
    }
}

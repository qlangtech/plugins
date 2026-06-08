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
package com.qlangtech.tis.plugin.ontology.chatbi.config;

import com.google.common.collect.Sets;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * SQL 校验配置
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class ValidationConfig implements Describable<ValidationConfig> {

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean enableExplain;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean enableKeywordCheck;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean enableAstCheck;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> allowedFirstKeywords;

    public static List<Option> getAllowedFirstKeywordsCandidate() {
        return dftAllowedFirstKeywords().stream().map(Option::new).toList();
    }

    public static List<String> dftAllowedFirstKeywords() {
        return List.of("SELECT", "WITH", "EXPLAIN", "SHOW", "DESC", "DESCRIBE");
    }

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> forbiddenKeywords;

    public static List<Option> getForbiddenKeywordsCandidate() {
        return dftForbiddenKeywords().stream().map(Option::new).toList();
    }

    public static List<String> dftForbiddenKeywords() {
        return List.of("DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
                "GRANT", "REVOKE", "EXEC", "EXECUTE", "CREATE", "REPLACE");
    }

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> safeFunctions;

    public static List<Option> getSafeFunctionsCandidate() {
        return dftSafeFunctions().stream().map(Option::new).toList();
    }

    public static List<String> dftSafeFunctions() {
        return List.of("REPLACE", "TRIM", "SUBSTRING", "CONCAT", "CAST", "CONVERT");
    }

    public boolean isEnableExplain() {
        return enableExplain != null ? enableExplain : true;
    }

    public boolean isEnableKeywordCheck() {
        return enableKeywordCheck != null ? enableKeywordCheck : true;
    }

    public boolean isEnableAstCheck() {
        return enableAstCheck != null ? enableAstCheck : true;
    }

    /**
     * 解析逗号分隔的字符串为 Set
     */
    public Set<String> getAllowedFirstKeywordsSet() {
        return parseCommaSeparated(allowedFirstKeywords);
    }

    public Set<String> getForbiddenKeywordsSet() {
        return parseCommaSeparated(forbiddenKeywords);
    }

    public Set<String> getSafeFunctionsSet() {
        if (CollectionUtils.isEmpty(safeFunctions)) {

        }
        return parseCommaSeparated(safeFunctions);
    }

    private Set<String> parseCommaSeparated(List<String> values) {
        return Sets.newHashSet(values);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ValidationConfig> {
        @Override
        public String getDisplayName() {
            return "Validation Config";
        }
    }
}

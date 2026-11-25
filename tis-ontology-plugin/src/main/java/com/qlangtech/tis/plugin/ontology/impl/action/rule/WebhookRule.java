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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Webhook 规则
 *
 * Action 执行后调用外部 Webhook API
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class WebhookRule extends OntologyActionRule {

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String webhookUrl;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public HttpMethod httpMethod = HttpMethod.POST;

    /**
     * 请求头键值行；值里可以写 {@code ${paramName}}。
     */
    @FormField(ordinal = 2, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,
            desClazz = HttpHeaderConfig.class, validate = {})
    public List<HttpHeaderConfig> headers;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
    public String bodyTemplate;

    @FormField(ordinal = 4, type = FormFieldType.DURATION_OF_SECOND, validate = {})
    public Duration timeoutSeconds ;

    /**
     * 失败重试策略。
     *
     * <p>多态字段（布尔开关的多态 Describable 化），TIS 自动检测为嵌套子表单：
     * <ul>
     *   <li>{@link NoRetryPolicy} - 不重试（默认）</li>
     *   <li>{@link AutoRetryPolicy} - 自动重试（固定策略：延迟重试一次，仅网络错误或 5xx 触发）</li>
     * </ul>
     */
    @FormField(ordinal = 5, validate = {Validator.require})
    public RetryPolicy retryOnFailure = new NoRetryPolicy();

    @Override
    public RuleType getRuleType() {
        return RuleType.WEBHOOK;
    }

    @Override
    public String identityValue() {
        return "webhook";
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = initLiteria();
        literia.add(new UDFDesc("webhookUrl", this.webhookUrl));
        literia.add(new UDFDesc("httpMethod", this.httpMethod == null ? null : this.httpMethod.name()));
        literia.add(new UDFDesc("headers", HttpHeaderConfig.literia(this.headers)));
        // 前端 tuples 视图对超长值自带 4 行 clamp + 展开，bodyTemplate 再长也安全
        literia.add(new UDFDesc("bodyTemplate", this.bodyTemplate));
        literia.add(new UDFDesc("timeoutSeconds"
                , this.timeoutSeconds == null ? null : String.valueOf(this.timeoutSeconds.getSeconds())));
        // 重试策略同样是多态字段，半配置时为 null，跳过该对
        if (this.retryOnFailure != null) {
            literia.add(new UDFDesc("retryOnFailure", this.retryOnFailure.getLiteria()));
        }
        return literia;
    }

    @Override
    public void accept(AdapterRuleVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * 把请求头按当前参数池渲染成「头名 → 头值」映射，保持配置顺序、同名后者覆盖前者。
     *
     * <p>头值里的 {@code ${paramName}} 占位符经 {@link #replaceTemplate} 替换
     * （头行为 null 跳过，头值缺失按空串处理）；未配置任何请求头时返回空 Map。
     *
     * @param parameters Action 参数池的运行时取值
     */
    public Map<String, String> renderHeaders(Map<String, Object> parameters) {
        Map<String, String> headerMap = new LinkedHashMap<>();
        if (headers == null) {
            return headerMap;
        }
        for (HttpHeaderConfig header : headers) {
            if (header == null || StringUtils.isEmpty(header.name)) {
                continue;
            }
            headerMap.put(header.name, replaceTemplate(header.value, parameters));
        }
        return headerMap;
    }

    /**
     * 渲染请求体：{@code ${paramName}} 占位符替换后的原文；未配置模板时返回空串。
     *
     * @param parameters Action 参数池的运行时取值
     */
    public String renderBody(Map<String, Object> parameters) {
        return replaceTemplate(bodyTemplate, parameters);
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.WEBHOOK;
        }
    }
}

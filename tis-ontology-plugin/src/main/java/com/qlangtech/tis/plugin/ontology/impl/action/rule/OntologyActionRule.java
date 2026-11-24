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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.manage.common.PropertyPlaceholderHelper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ontology Action Rule 插件接口
 * <p>
 * 定义 Action 的执行规则，包括：
 * - Ontology Rules: CREATE_OBJECT, MODIFY_OBJECT, DELETE_OBJECT, CREATE_LINK, DELETE_LINK
 * - Function Rules: FUNCTION_BACKED
 * - Side Effect Rules: NOTIFICATION, WEBHOOK, WRITEBACK_WEBHOOK
 * - Advanced Rules: SCHEDULE, APPLY_SCENARIO
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public abstract class OntologyActionRule implements Describable<OntologyActionRule> {

    public static List<BaseRuleDescriptor> descFilter(List<BaseRuleDescriptor> descs, String ruleType) {
        final RuleGroup ruleGroup = RuleGroup.valueOf(ruleType);
        return descs.stream().filter((desc) -> {
            return ruleGroup == desc.getRuleGroup();
        }).toList();
    }

    /**
     * 获取规则类型
     */
    public abstract RuleType getRuleType();

    /**
     * 获取唯一标识值
     */
    public abstract String identityValue();

    /**
     * 是否启用该规则
     */
    public boolean isEnabled() {
        return true;
    }

//    /**
//     * 执行规则
//     *
//     * @param context 执行上下文
//     * @return 执行结果
//     */
//    public abstract RuleExecutionResult execute(RuleExecutionContext context) throws Exception;

    /**
     * 验证规则配置是否有效
     *
     * @return 验证通过返回 true
     */
    public boolean validate() {
        return true;
    }

    /**
     * 把结构化的映射行解析成「目标字段 → 取值」。
     *
     * <p>原先 {@link CreateObjectRule}、{@link ModifyObjectRule}、{@link FunctionBackedRule}
     * 各有一份逐字重复的 parse + build 两段实现，这里收敛成一处。
     *
     * <p>2026/10/12 重构：改用 {@link ValueSourceConfig#resolveValue} 多态方法，
     * 消除 switch-case 分支。
     *
     * <p>2026/11/24 改为 public static 并接入执行链：对象规则（Create/Modify）的映射行此前
     * 只有 {@code DefaultOntologyAction#collectRuleProperties} 在展示侧消费，执行期
     * {@code ObjectActionType#mergePropertyMappings} 用本方法把「参数池里没有的属性」按映射行补齐
     * （fill-gap 语义，表单已提供的值优先）。{@code FunctionBackedRule} 的形参映射同样走这里。
     *
     * @param mappings   映射行，可为 null（函数可以无入参）
     * @param parameters Action 参数池的运行时取值（多对象类型 Action 下 key 是带命名空间前缀的全名）
     * @return 目标字段到取值的映射；解析结果为 null 时该行被跳过（如参数不存在）
     */
    public static Map<String, Object> buildMappedValues(List<PropertyMappingConfig> mappings,
                                                        Map<String, Object> parameters) {
        Map<String, Object> result = new java.util.HashMap<>();
        if (mappings == null) {
            return result;
        }
        for (PropertyMappingConfig m : mappings) {
            if (m.source != null) {
                Object value = m.source.resolveValue(parameters);
                if (value != null) {
                    result.put(m.target, value);
                }
            }
        }
        return result;
    }

    /**
     * 把用户提交的表单实例的 public 实例字段读成「参数名 → 值」。
     *
     * <p>生成的运行时表单（{@code ActionFormPreview}）字段名就是 Action 参数名
     * （见 {@link com.qlangtech.tis.plugin.ontology.impl.action.ActionFormDescriptorBuilder}），
     * 因此这里按字段名平铺成参数 Map，映射行解析、模板替换等可沿用参数池时代的逻辑。
     */
    protected static Map<String, Object> getParameterValues(Describable<?> plugin) {
        Map<String, Object> parameters = new HashMap<>();
        if (plugin == null) {
            return parameters;
        }
        for (Field field : plugin.getClass().getFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                parameters.put(field.getName(), field.get(plugin));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("can not read plugin form field:" + field.getName(), e);
            }
        }
        return parameters;
    }

    /**
     * 取参数值并转成字符串；参数缺失返回 null。表单字段可能是 Integer/Long 等类型，统一按展示语义转换。
     */
    protected static String getStringParameter(Map<String, Object> parameters, String name) {
        Object value = parameters.get(name);
        return value == null ? null : String.valueOf(value);
    }

    /**
     * 替换模板中的参数占位符，格式: {@code ${paramName}}。
     *
     * <p>复用 tis-config 的 {@link PropertyPlaceholderHelper}（默认 {@code ${}/{@code }} 前后缀、
     * 忽略无法解析的占位符），与 NotificationRule.md / WebhookRule.md 中声明的占位符替换规则一致：
     * 参数名大小写敏感、参数不存在时占位符保持原样、参数值经 {@code toString()} 转换。
     *
     * <p>原先 {@link NotificationRule} 私有一份字符串逐个 {@code replace} 的实现——参数值里再含
     * 占位符时，替换结果还依赖 Map 遍历顺序；收敛到基类后由 helper 确定性递归解析
     * （{@link WebhookRule} 的请求头值、请求体模板同样走这里）。
     *
     * @param template   模板原文，null 按空串处理
     * @param parameters Action 参数池的运行时取值
     */
    protected static String replaceTemplate(String template, Map<String, Object> parameters) {
        if (template == null) {
            return "";
        }

        return PropertyPlaceholderHelper.replace(template, (placeholderName) -> {
            Object val = parameters.get(placeholderName);
            return val == null ? null : val.toString();
        });
    }

//    /**
//     * 执行用户提交的form 表单
//     *
//     * @param plugin
//     */
//    public abstract void execute(Describable<?> plugin);

    /**
     * 访问者模式的双分发入口：具体子类回调 visitor 上对应的 visit 重载。
     *
     * @see RuleVisitor
     */
    public abstract void accept(AdapterRuleVisitor visitor);

    /**
     * 规则基础 Descriptor
     */
    public static abstract class BaseRuleDescriptor extends Descriptor<OntologyActionRule> implements DescriptorUseableShortComment {

        /**
         * 获取规则类型
         */
        public abstract RuleType getRuleType();

        /**
         * 获取规则分组
         */
        public RuleGroup getRuleGroup() {
            return getRuleType().getGroup();
        }

        @Override
        public final String getDisplayName() {
            return getRuleType().name();
        }

        @Override
        public final String shortComment() {
            return getRuleType().getDescription();
        }
    }
}
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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Action 参数校验脚本生成专用的 {@link BlockScriptBuffer} 派生类。
 *
 * <p>各 {@link com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType}
 * 子类实现 {@code generateParamVaildateScript} 时有两类脚本片段会随控件铺开大量重复，
 * 收拢到这里：
 * <ol>
 *   <li>{@code validateXxx} 方法的 4 参固定签名——是 {@code Descriptor#createValidateMap}
 *       反射注册的硬契约（参数类型 {@code ==} 精确比对 + primitive boolean 返回，
 *       首参写成子类 IControlMsgHandler 会导致方法<b>静默不注册</b>）；</li>
 *   <li>{@code msgHandler.addFieldError(context, fieldName, '...')} 调用形态——实参名与
 *       签名里的固定形参名对齐。</li>
 * </ol>
 *
 * <p>所有 helper 拼出的脚本都依赖 {@link #validateMethod} 固定的形参名
 * （{@code msgHandler}/{@code context}/{@code fieldName}/{@code value}），改签名时必须同步。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/22
 * @see ActionFormDescriptorBuilder 创建入口
 */
public class ValidateScriptBuffer extends BlockScriptBuffer {

    /**
     * Desc 类成员在生成源码里缩进 8 空格（与 {@code ActionFormDescriptorBuilder}
     * 拼接 sb 时的字面量对齐）。
     */
    public ValidateScriptBuffer() {
        super(8);
    }

    /**
     * 生成 {@code public boolean validate<FieldId>(IFieldErrorHandler msgHandler, Context context,
     * String fieldName, String value) { ... }}。
     *
     * @param fieldId 参数字段名（{@code ActionParameterConfig#requireFieldName()}），
     *                方法名后缀首字母大写后须能被 {@code createValidateMap} uncapitalize 回字段名
     * @param body    方法体，lambda 里可直接调用本类的 helper
     */
    public void validateMethod(String fieldId, IValidateBody body) {
        this.methodBody("public boolean " + Descriptor.KEY_VALIDATE_METHOD_PREFIX
                        + StringUtils.capitalize(fieldId)
                        + "(IFieldErrorHandler msgHandler, Context context, String fieldName, String value)",
                (script) -> body.execute(this));
    }

    /**
     * 把自由输入文本安全嵌入 groovy 单引号字面量：反斜杠与单引号转义。
     *
     * <p>凡是要把用户可自由编辑的配置值（如枚举候选项、日期 pattern）拼进生成脚本，
     * 都必须先过本方法，否则值里的单引号会打碎 groovy 字面量让整份脚本编译失败。
     */
    public static String escapeGroovyStr(String input) {
        return StringUtils.replace(StringUtils.replace(input, "\\", "\\\\"), "'", "\\'");
    }

    /**
     * 起一行 {@code msgHandler.addFieldError(context, fieldName, '...')}。
     *
     * @param msg 错误文案，只放烘焙字面量/固定中文（含单引号会被 {@link #escapeGroovyStr}
     *            自动转义，不会打碎脚本）；仍不得拼 displayName 等自由输入——不是为了防单引号，
     *            而是避免生成结果随实例漂移
     */
    public void addFieldError(String msg) {
        this.startLine("msgHandler.addFieldError(context, fieldName, '" + escapeGroovyStr(msg) + "')");
    }

    /**
     * 守卫块：{@code if (condition) { addFieldError(msg); return false }}——校验不过即拒绝。
     */
    public void addFieldErrorIf(String condition, String msg) {
        this.methodBody("if (" + condition + ")", (script) -> {
            this.addFieldError(msg);
            this.startLine("return false");
        });
    }

    /**
     * 候选项成员守卫：{@code if (!(value in ['a','b'])) { addFieldError(msg); return false }}。
     *
     * @param candidates 候选值列表，元素是用户可自由编辑的配置值，逐个过 {@link #escapeGroovyStr}
     */
    public void rejectIfNotIn(List<String> candidates, String msg) {
        String joined = candidates.stream()
                .map((c) -> "'" + escapeGroovyStr(c) + "'").collect(Collectors.joining(","));
        this.addFieldErrorIf("!(value in [" + joined + "])", msg);
    }

    /**
     * 严格日期/时间解析守卫：{@code SimpleDateFormat} 关闭 lenient + {@code ParsePosition}
     * 要求整串被消费，拒绝「2026-01-01xxx」这类尾部夹带垃圾的输入——parse(String) 单参形态
     * 会静默忽略尾部文本，防不住。
     *
     * <p>刻意用 {@code parse(value, pos)} 两参形态而非 try/catch：解析失败返回 null 不抛异常，
     * 生成脚本里可以不用 try 块。
     */
    public void rejectInvalidDate(String pattern, String msg) {
        this.startLine("java.text.ParsePosition pos = new java.text.ParsePosition(0)");
        this.startLine("java.text.SimpleDateFormat dateFmt = new java.text.SimpleDateFormat('"
                + escapeGroovyStr(pattern) + "')");
        this.startLine("dateFmt.lenient = false");
        this.methodBody("if (dateFmt.parse(value, pos) == null || pos.index != value.length())", (script) -> {
            this.addFieldError(msg);
            this.startLine("return false");
        });
    }

    /**
     * 与父类 {@link IFuncFormatCall} 同形的函数接口，区别是 {@code execute} 收到的是
     * 本派生类型，lambda 里可以直接调用 {@link #addFieldError} 等 helper，
     * 无需在 19 个 widget 里反复强转。
     */
    public interface IValidateBody {

        void execute(ValidateScriptBuffer script);
    }
}

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

package com.qlangtech.tis.plugin.ontology.impl.action.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Action 参数在最终用户表单里用什么控件收集（聚合属性插件基类）。
 *
 * <p>上一版这里是一个裸的 {@link FormFieldType} 枚举字段，承载不了控件自己的配置——
 * 文本框要 placeholder/默认值/校验规则，数字要 min/max，下拉要选项表，日期要格式。
 * 这些字段平铺在宿主类上就只在某个枚举值下有语义，只能靠 {@code validateAll()} 里的 if 兜底。
 * 改为多态后，每种控件成为独立插件，各自持有专属字段，有效性由编译器保证。
 *
 * <p>子类与 {@link FormFieldType} 的常量一一对应（19 个），但**不存在双重分类体系**：
 * 「是哪种控件」的唯一事实来源是子类的 Java 类型；{@link FormFieldType} 只作为 core 的
 * 注解读数（所有 {@code @FormField} 都在用）与下发给前端的线格式 token 存在，
 * 不再是宿主类上的字段。
 *
 * <p><b>基类刻意不声明任何 {@code @FormField} 字段</b>（与 {@code ActionType} /
 * {@code ActionParameterDataType} 一致）：字段全在子类，各子类的 ordinal 自 {@code 0} 起算，
 * 也就不存在「基类与子类 ordinal 撞区间导致表单渲染顺序飘」的问题。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 * @see com.qlangtech.tis.plugin.ontology.impl.action.PrimitiveDataType 的唯一使用方
 */
public abstract class UserInputWidgetType implements Describable<UserInputWidgetType> {

    /**
     * 本控件对应的 TIS 表单控件种类。除子类自身外，它还是下发给前端的渲染派发键。
     */
    public abstract FormFieldType getFormFieldType();

    /**
     * 返回输入属性的默认值
     *
     * @return
     */
    public abstract Object getDefaultValue();

    /**
     * 把本控件专属的 extra-props 追加到 {@code fieldJson}。
     *
     * <p>{@code label}/{@code help}/{@code dftVal} 这些宿主级属性由
     * {@code ActionParameterConfig#appendExtraPropsJson} 负责，这里只负责控件自己才知道的属性：
     * 比如下拉类控件的 {@code enum} 候选表。基类默认什么都不做，带 {@code options} 的子类覆写。
     */
    public final void setExtraPropsJson(JSONObject fieldJson) {
        if (this.getDefaultValue() != null) {
            fieldJson.put(com.qlangtech.tis.extension.util.PluginExtraProps.KEY_DFTVAL_PROP, this.getDefaultValue());
        }
        this.appendExtraPropsJson(fieldJson);
    }

    /**
     * 追加属性描述文件中额外描述星系
     *
     * @param fieldJson
     */
    protected abstract void appendExtraPropsJson(JSONObject fieldJson);

    /**
     * 候选选项列表 → 前端线格式的 {@link Option} 列表（{@code label} 缺省回落 {@code val}）。
     */
    public static List<Option> toOptions(List<WidgetOptionConfig> options) {
        if (options == null) {
            return List.of();
        }
        List<Option> result = new ArrayList<>(options.size());
        for (WidgetOptionConfig cfg : options) {
            if (cfg != null) {
                result.add(new Option(StringUtils.isNotEmpty(cfg.label) ? cfg.label : cfg.val, cfg.val));
            }
        }
        return result;
    }

//    /**
//     * 带 {@code validators} 字段的子类共用的候选校验规则。
//     *
//     * <p>公开静态而非 protected：取用位置是子类里嵌套的 Descriptor，而那个嵌套类并不是本类的子类，
//     * protected 访问会跨不过包边界（{@code widget} 与 {@code widget.impl} 是两个包）。
//     *
//     * <p>已知限制：{@link Validator} 没有中文标签，这里 label 与 val 都取枚举常量名。
//     * 要显示中文需后续在对应子类的 .json 里声明 {@code enum} 数组覆盖。
//     */
//    public static List<Option> validatorOptions() {
//        return Arrays.stream(Validator.values())
//                .map((v) -> new Option(v.name(), v.name()))
//                .toList();
//    }

    /**
     * 聚合属性插件的基础 Descriptor，所有子类的 Descriptor 必须继承此类。
     */
    protected abstract static class BasicDescriptor extends Descriptor<UserInputWidgetType>
            implements DescriptorUseableShortComment, IEndTypeGetter {

        public abstract FormFieldType getFormFieldType();

        /**
         * 中文功能说明与选项图标都直接转调 {@link FormFieldType} —— 19 个子类描述符因此零样板，
         * 说明与图标只声明一次，不会出现「子类各写一份、日后漂移」的情况。
         */
        @Override
        public final String shortComment() {
            return getFormFieldType().shortComment();
        }

        @Override
        public final EndType getEndType() {
            return getFormFieldType().getEndType();
        }

        @Override
        public Map<String, Object> getExtractProps(boolean forAIPromote) {
            Map<String, Object> props = super.getExtractProps(forAIPromote);
            // 必须放 String：extractProps 会被 DescriptorsJSON 原样交给 fastjson 序列化，
            // 枚举会被写成前端认不出的形态，渲染派发键就失效了。
            props.put(KEY_FORM_FIELD_TYPE, getFormFieldType().name());
            return props;
        }
    }

    /**
     * 下发给前端的控件种类短 key，取值即 {@link FormFieldType} 的常量名。
     * 前端据此派发到已有的渲染器，无需再维护一张「实现类 FQCN → 控件」的映射表。
     */
    public static final String KEY_FORM_FIELD_TYPE = "formFieldType";
}

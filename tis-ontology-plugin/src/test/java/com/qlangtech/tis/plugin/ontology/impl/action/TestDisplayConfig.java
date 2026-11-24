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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@link DisplayConfig} 这一族多态字段的结构与行为回归测试（与 {@link TestCustomizeContent}
 * 同一套测试基建，全程不依赖 TIS 运行期）。
 *
 * <p>钉住五条约定：
 * <ol>
 *   <li>恰好两个具体子类、关态子类 displayName == {@code SWITCH_OFF}；</li>
 *   <li>宿主 {@code ActionParameters.json} 的 {@code dftVal} 逐字等于 {@code "off"}
 *       （写成别的字符串默认项静默漂移）；</li>
 *   <li>{@link DisplayConfig#runtimeProps} 的扁平化口径 —— off/null → {@code null}，
 *       on → 前端 {@code ActionDisplayConfig} 逐字对应的两个 key；</li>
 *   <li>{@code formWidth} 的 {@code positive_integer} 校验真拦得住 0 / 负数 / 非整数；</li>
 *   <li>{@code formWidth} 的上限 {@link DisplayConfigOn#MAX_FORM_WIDTH} 由 descriptor 的
 *       {@code validateFormWidth} 真拦得住超限值。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/21
 */
public class TestDisplayConfig {

    private static final String ACTION_PKG = "com.qlangtech.tis.plugin.ontology.impl.action";
    private static final String ACTION_PARAMETERS = ACTION_PKG + ".ActionParameters";

    /**
     * 恰好两个具体子类，且关态是 {@link DisplayConfigOff}（理由同 {@code TestCustomizeContent}：
     * 多一个子类就在表单 impl 下拉里多出一个无法预期的选项）。
     */
    @Test
    public void exactlyTwoSubclassesOneOfWhichIsOff() {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(DisplayConfig.class, ACTION_PKG);
        Set<String> names = TisPluginFormTestSupport.names(subs);
        Assert.assertEquals("DisplayConfig 应恰好有两个具体子类（一个关闭、一个自定义），实际：" + names,
                new TreeSet<>(List.of("DisplayConfigOff", "DisplayConfigOn")), names);

        List<String> offs = new ArrayList<>();
        for (Class<?> sub : subs) {
            if (Descriptor.SWITCH_OFF.equals(TisPluginFormTestSupport.descriptorOf(sub).getDisplayName())) {
                offs.add(sub.getSimpleName());
            }
        }
        Assert.assertEquals("应恰好有一个子类的 displayName 是 SWITCH_OFF（前端按它选默认项），实际是：" + offs,
                1, offs.size());
        Assert.assertEquals("关闭子类应当是 DisplayConfigOff", "DisplayConfigOff", offs.get(0));
    }

    @Test
    public void offAndOnDescriptorNamesAreDistinct() {
        Assert.assertEquals("关态子类的 displayName 应当就是框架常量 SWITCH_OFF",
                Descriptor.SWITCH_OFF, TisPluginFormTestSupport.descriptorOf(DisplayConfigOff.class).getDisplayName());
        Assert.assertEquals("开态子类的 displayName 应当就是框架常量 SWITCH_ON",
                Descriptor.SWITCH_ON, TisPluginFormTestSupport.descriptorOf(DisplayConfigOn.class).getDisplayName());
    }

    /**
     * {@code ActionParameters.json} 里 {@code displayConfig} 的 {@code dftVal} 必须逐字是
     * {@code "off"}；宿主字段声明类型必须是抽象基类（写成具体子类就没法多态），且必须带
     * {@code readResolve} 兜底 —— 存量 {@code params/<actionName>.xml} 没有该元素时，
     * XStream 反序列化得 null，缺兜底就是运行时读参 NPE。
     */
    @Test
    public void hostFieldWiringIsComplete() throws Exception {
        JSONObject resource = TisPluginFormTestSupport.formResource(ACTION_PARAMETERS);
        Assert.assertNotNull("ActionParameters.json 应存在", resource);
        JSONObject meta = resource.getJSONObject("displayConfig");
        Assert.assertNotNull("displayConfig 在 ActionParameters.json 里应有条目", meta);
        Assert.assertEquals("dftVal 必须逐字等于关闭子类的 displayName（SWITCH_OFF）",
                Descriptor.SWITCH_OFF, meta.getString("dftVal"));

        Assert.assertTrue("DisplayConfig 应当是抽象基类",
                Modifier.isAbstract(DisplayConfig.class.getModifiers()));
        Assert.assertEquals("ActionParameters.displayConfig 应声明为抽象基类类型（多态）",
                DisplayConfig.class, ActionParameters.class.getField("displayConfig").getType());
        Assert.assertNotNull("必须声明 readResolve 兜底（XStream 不走构造器与字段初值）",
                ActionParameters.class.getDeclaredMethod("readResolve"));
    }

    /**
     * 零字段的关态子类<b>不该</b>有 json（写了是死文件）；开态子类必须有，且键集与
     * {@code @FormField} 字段集一致 —— 漏一个字段的表现是那个输入框没有 label / help，不报错。
     */
    @Test
    public void formResourcesMatchFormFields() {
        Assert.assertNull("DisplayConfigOff 零字段，不应有表单资源",
                TisPluginFormTestSupport.formResource(DisplayConfigOff.class.getName()));

        JSONObject resource = TisPluginFormTestSupport.formResource(DisplayConfigOn.class.getName());
        Assert.assertNotNull("DisplayConfigOn 有两个字段，应当有表单资源（否则输入框没有 label）", resource);
        Assert.assertEquals("json 的键集必须与 @FormField 字段集一致",
                new TreeSet<>(List.of("compactVerticalLayout", "formWidth")), new TreeSet<>(resource.keySet()));
    }

    /**
     * 扁平化口径：off / null → {@code null}（前端走框架缺省）；on → 恰好两个 key，
     * 键名与前端 {@code ActionDisplayConfig} 逐字对应，
     * {@code compactVerticalLayout} 经 {@code Boolean.TRUE.equals} 归一（null 按否处理）。
     */
    @Test
    public void runtimePropsFlattening() {
        Assert.assertNull("null（参数池未配置）不下发", DisplayConfig.runtimeProps(null));
        Assert.assertNull("off 形态不下发", DisplayConfig.runtimeProps(new DisplayConfigOff()));

        DisplayConfigOn on = new DisplayConfigOn();
        JSONObject props = DisplayConfig.runtimeProps(on);
        Assert.assertNotNull(props);
        Assert.assertEquals("缺省值应与 displayConfig 引入前模态的硬编码行为一致",
                Boolean.TRUE, props.getBoolean(DisplayConfig.KEY_COMPACT_VERTICAL_LAYOUT));
        Assert.assertEquals(600, props.getIntValue(DisplayConfig.KEY_FORM_WIDTH));

        on.compactVerticalLayout = false;
        on.formWidth = 800;
        props = DisplayConfig.runtimeProps(on);
        Assert.assertEquals(Boolean.FALSE, props.getBoolean(DisplayConfig.KEY_COMPACT_VERTICAL_LAYOUT));
        Assert.assertEquals(800, props.getIntValue(DisplayConfig.KEY_FORM_WIDTH));
    }

    /**
     * {@code formWidth} 的正整数校验必须真拦得住 0、负数、小数、前导零与非数字；
     * 空值按 {@code guardNotBlank} 语义放行（空白由 {@code Validator.require} 兜底）。
     */
    @Test
    public void positiveIntegerGuardRejectsNonPositiveWidth() {
        RecordingFieldErrorHandler handler = new RecordingFieldErrorHandler();
        Context context = new DefaultContext();

        Assert.assertTrue("合法宽度应通过", Validator.positive_integer.validate(handler, context, "formWidth", "600"));
        Assert.assertTrue("多位数应通过", Validator.positive_integer.validate(handler, context, "formWidth", "1280"));
        Assert.assertTrue("空值跳过（require 兜底）",
                Validator.positive_integer.validate(handler, context, "formWidth", ""));

        List<String> rejected = new ArrayList<>();
        for (String bad : List.of("0", "-5", "3.5", "0123", "abc", " 600 ")) {
            if (!Validator.positive_integer.validate(handler, context, "formWidth", bad)) {
                rejected.add(bad);
            }
        }
        Assert.assertEquals("全部非法输入都必须被拦下，实际放行了：" + rejected,
                List.of("0", "-5", "3.5", "0123", "abc", " 600 "), rejected);
        Assert.assertEquals("每次拦截都应记录一条字段错误", 6, handler.errors.size());
    }

    /**
     * 上限约束必须由 descriptor 的 {@code validateFormWidth} 拦住（框架按方法名反射挂接到
     * {@code formWidth} 字段，{@code Descriptor#createValidateMap}）。顺带钉住两点挂接语义：
     * 非整数值进来不抛异常也不重复报错（标准 {@code Validator} 已报过）；错误挂在传入的
     * {@code fieldName} 上（嵌套字段路径前缀由 fieldStack 拼，方法内不感知）。
     */
    @Test
    public void formWidthUpperBoundRejectedByDescriptorValidator() {
        DisplayConfigOn.DefaultDescriptor descriptor =
                (DisplayConfigOn.DefaultDescriptor) TisPluginFormTestSupport.descriptorOf(DisplayConfigOn.class);
        RecordingFieldErrorHandler handler = new RecordingFieldErrorHandler();
        Context context = new DefaultContext();

        Assert.assertTrue("上限值本身应放行", descriptor.validateFormWidth(handler, context, "formWidth", "900"));
        Assert.assertTrue("合法值应放行", descriptor.validateFormWidth(handler, context, "formWidth", "600"));
        Assert.assertEquals("放行不该记录错误", 0, handler.errors.size());

        Assert.assertFalse("超过上限必须被拦下",
                descriptor.validateFormWidth(handler, context, "formWidth", "901"));
        Assert.assertFalse("远超上限也必须被拦下",
                descriptor.validateFormWidth(handler, context, "formWidth", "4096"));
        Assert.assertEquals("每次拦截都应记录一条字段错误", 2, handler.errors.size());
        Assert.assertTrue("错误应挂在 formWidth 字段上", handler.errors.stream().allMatch(e -> e.startsWith("formWidth:")));

        // 非整数值也会进到本方法（挂接点不因标准校验失败而跳过）：吞解析异常、不重复报错
        Assert.assertTrue("非整数值不该由本方法报错", descriptor.validateFormWidth(handler, context, "formWidth", "abc"));
        Assert.assertEquals("非整数不新增字段错误", 2, handler.errors.size());
    }

    private static class RecordingFieldErrorHandler implements IFieldErrorHandler {
        final List<String> errors = new ArrayList<>();

        @Override
        public void addFieldError(Context context, String fieldName, String msg, Object... params) {
            this.errors.add(fieldName + ":" + msg);
        }

        @Override
        public boolean validateBizLogic(BizLogic logicType, Context context, String fieldName, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BasicPipelineValidator getPipelineValidator(BizLogic logicType) {
            throw new UnsupportedOperationException();
        }
    }
}

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

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@link CustomizeContent} 这一族多态字段的结构回归测试。
 *
 * <p>测试全程<b>不依赖 TIS 运行期</b>（走 {@link TisPluginFormTestSupport}），所以在本工程
 * 默认的 surefire 配置下就能真跑。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/14
 */
public class TestCustomizeContent {

    private static final String ACTION_PKG = "com.qlangtech.tis.plugin.ontology.impl.action";
    private static final String OBJECT_ACTION_TYPE = "com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType";

    /**
     * 恰好两个具体子类，且其中一个的 {@code displayName} 就是 {@link Descriptor#SWITCH_OFF}。
     *
     * <p>「关态子类的 displayName == SWITCH_OFF」是这套开关机制的<b>全部</b>约定：前端拿
     * {@code .json} 里的 {@code dftVal} 逐项比对各子描述符的 displayName 来选默认项，
     * 多一个子类（例如有人给抽象基类也加上 {@code @TISExtension}）就会在表单上多出一个
     * 无法预期的选项。
     */
    @Test
    public void exactlyTwoSubclassesOneOfWhichIsOff() {
        Set<Class<?>> subs = TisPluginFormTestSupport.concreteSubclassesOf(CustomizeContent.class, ACTION_PKG);
        Set<String> names = TisPluginFormTestSupport.names(subs);
        Assert.assertEquals("CustomizeContent 应恰好有两个具体子类（一个关闭、一个填文案），实际：" + names,
                new TreeSet<>(List.of("DefaultCustomizeContent", "NoneCustomizeContent")), names);

        List<String> offs = new ArrayList<>();
        for (Class<?> sub : subs) {
            if (Descriptor.SWITCH_OFF.equals(TisPluginFormTestSupport.descriptorOf(sub).getDisplayName())) {
                offs.add(sub.getSimpleName());
            }
        }
        Assert.assertEquals("应恰好有一个子类的 displayName 是 SWITCH_OFF（前端按它选默认项），实际是：" + offs,
                1, offs.size());
        Assert.assertEquals("关闭子类应当是 NoneCustomizeContent", "NoneCustomizeContent", offs.get(0));
    }

    /**
     * 关态子类描述符的 displayName 必须逐字是框架常量 {@code SWITCH_OFF}（{@code "off"}），
     * 开态子类则不是 —— 两者不能撞名，否则表单上出现两个同名选项，用户无从分辨。
     */
    @Test
    public void offAndOnDescriptorNamesAreDistinct() {
        Assert.assertEquals("关态子类的 displayName 应当就是框架常量 SWITCH_OFF",
                Descriptor.SWITCH_OFF, TisPluginFormTestSupport.descriptorOf(NoneCustomizeContent.class).getDisplayName());
        Assert.assertEquals("开态子类的 displayName 应当就是框架常量 SWITCH_DEFAULT",
                Descriptor.SWITCH_DEFAULT, TisPluginFormTestSupport.descriptorOf(DefaultCustomizeContent.class).getDisplayName());
    }

    /**
     * {@link CustomizeContent#content()} 的口径：没定制（关态）恒空；开态下内容为 null、
     * 空串、纯空白都当「没有定制」处理 —— 调用方据此回退到框架缺省文案，而不是把空白
     * 当成一段合法文案渲染出来。
     */
    @Test
    public void contentIsEmptyUnlessActuallyCustomized() {
        Assert.assertTrue("关态子类恒为空", new NoneCustomizeContent().content().isEmpty());

        DefaultCustomizeContent customized = new DefaultCustomizeContent();
        Assert.assertTrue("内容为 null 视为没有定制", customized.content().isEmpty());

        customized.content = "   ";
        Assert.assertTrue("纯空白视为没有定制", customized.content().isEmpty());

        customized.content = "  提交工单  ";
        Assert.assertEquals("应当去掉首尾空白后返回", "提交工单", customized.content().orElse(null));
    }

    /**
     * 两个字段都必须声明在 {@link ObjectActionType} 上，且类型是<b>抽象基类</b>
     * （写成具体子类就没法多态了），基类本身也不该被实例化。
     */
    @Test
    public void objectActionTypeDeclaresBothFieldsAsPolymorphic() throws Exception {
        Assert.assertTrue("CustomizeContent 应当是抽象基类",
                Modifier.isAbstract(CustomizeContent.class.getModifiers()));

        for (String field : List.of("submitButton", "successMessage")) {
            Assert.assertEquals("ObjectActionType." + field + " 应声明为抽象基类类型（多态）",
                    CustomizeContent.class, ObjectActionType.class.getField(field).getType());
        }
    }

    /**
     * {@code ObjectActionType.json} 里两个字段的 {@code dftVal} 必须逐字等于关闭子类的
     * {@code displayName}（{@code "off"}）。
     *
     * <p>写成 {@code "OFF"} / {@code "关闭"} 都不报错，只会静默落到开态子类上，于是
     * 「新建的 Action 默认不定制文案」这个约定无声失效。
     */
    @Test
    public void hostFormResourceDefaultsToTheOffSubclass() {
        JSONObject resource = TisPluginFormTestSupport.formResource(OBJECT_ACTION_TYPE);
        Assert.assertNotNull("ObjectActionType.json 应存在", resource);

        String offDisplayName = TisPluginFormTestSupport.descriptorOf(NoneCustomizeContent.class).getDisplayName();
        for (String field : List.of("submitButton", "successMessage")) {
            JSONObject meta = resource.getJSONObject(field);
            Assert.assertNotNull(field + " 在 ObjectActionType.json 里应有条目", meta);
            Assert.assertEquals("dftVal 必须逐字等于关闭子类的 displayName",
                    offDisplayName, meta.getString("dftVal"));
            Assert.assertEquals("关闭子类的 displayName 应当就是框架常量 SWITCH_OFF",
                    Descriptor.SWITCH_OFF, meta.getString("dftVal"));
        }
    }

    /**
     * 零字段的关态子类<b>不该</b>有 json（写了是死文件：TIS 只在类上存在 {@code @FormField}
     * 时才去查同名资源）；有字段的开态子类<b>必须</b>有，且键集与字段集一致 ——
     * 漏一个字段的表现是「那个输入框没有 label / help」，不报错。
     */
    @Test
    public void formResourcesMatchFormFields() {
        Assert.assertNull("NoneCustomizeContent 零字段，不应有表单资源",
                TisPluginFormTestSupport.formResource(NoneCustomizeContent.class.getName()));

        JSONObject resource = TisPluginFormTestSupport.formResource(DefaultCustomizeContent.class.getName());
        Assert.assertNotNull("DefaultCustomizeContent 有 content 字段，应当有表单资源（否则输入框没有 label）", resource);
        Assert.assertEquals("json 的键集必须与 @FormField 字段集一致",
                new TreeSet<>(List.of("content")), new TreeSet<>(resource.keySet()));
    }
}

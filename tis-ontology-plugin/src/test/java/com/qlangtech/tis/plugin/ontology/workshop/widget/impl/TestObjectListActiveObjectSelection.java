package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.widget.DefaultObjectSelection;
import com.qlangtech.tis.plugin.ontology.workshop.widget.NoObjectSelection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code ObjectListWidget} 的「单选」契约。
 *
 * <h3>为什么需要这个测试</h3>
 * 前端此前只能靠「点整行」单选，且<b>没有任何视觉反馈</b> —— 用户看不出哪一行是当前选中项，
 * 也看不出可以点。Palantir 的 Object List 同样是「点行 + 高亮 active row」而不是 radio，
 * 缺失的正是「高亮」这一半。本轮前端补上了高亮，本测试钉住与之配套的两条后端契约。
 *
 * <h3>三块断言</h3>
 * <ol>
 *   <li>{@link #activeObjectVarStaysOnTheHost()} —— <b>{@code activeObjectVar} 必须留在宿主上</b>，
 *       不下沉进 {@link NoObjectSelection}。它是本 Widget 的基础输出，与多选开关无关
 *       （Palantir：「Active object」是两个输出变量之一，多选只管第二个 Selected objects）。
 *       下沉进关态子类会让开启多选时取不到该字段，只能在两个子类各放一份才能保持行为。</li>
 *   <li>{@link #autoSelectDefaultsToOn()} —— {@code autoSelectActiveObject} 的 Java 初值是
 *       {@code TRUE}、{@code null}（存量 XML 缺该元素）同样按开启处理。
 *       这条与 Palantir 默认行为一致（加载时自动选中第一行）。</li>
 *   <li>{@link #jsonDeclaresTrueDftVal()} —— json 的 {@code dftVal} 必须是<b>真布尔 true</b>
 *       且与 Java 初值一致。写成字符串 {@code "true"} 时控件初始化不会勾选，
 *       表现是「新建的 widget 默认不自动选中」—— 与设计相反且不报错。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/12
 */
public class TestObjectListActiveObjectSelection {

    // ===================================================================
    // 1. activeObjectVar 的归属
    // ===================================================================

    /**
     * {@code activeObjectVar} 与 {@code autoSelectActiveObject} 都在宿主上，且关闭态子类零字段。
     *
     * <p>反过来写（把字段搬进 {@link NoObjectSelection}）会让
     * {@link DefaultObjectSelection} 下拿不到「当前选中对象」，与「单选始终可用」相悖；
     * 且要为此引入 XStream 迁移，风险远大于收益。详见 {@link NoObjectSelection} 的类注释。
     */
    @Test
    public void activeObjectVarStaysOnTheHost() throws Exception {
        Field activeVar = ObjectListWidget.class.getField(ObjectListWidget.KEY_ACTIVE_OBJECT_VAR);
        Assert.assertEquals("activeObjectVar 应声明为 String 并留在宿主上",
                String.class, activeVar.getType());

        Field autoSelect = ObjectListWidget.class.getField(ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT);
        Assert.assertEquals("autoSelectActiveObject 应是 Boolean 包装类型（存量 XML 缺元素时为 null）",
                Boolean.class, autoSelect.getType());

        Assert.assertNull("单选相关字段不得下沉进 NoObjectSelection —— 它是零字段的关态标记类",
                fieldOrNull(NoObjectSelection.class, ObjectListWidget.KEY_ACTIVE_OBJECT_VAR));
        Assert.assertNull("同上：autoSelectActiveObject 也不该在关态子类里",
                fieldOrNull(NoObjectSelection.class, ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT));
    }

    /**
     * 关态子类必须完全没有 {@code @FormField}：一旦有了字段而没有同名 json，
     * 那个输入框就没有 label / help（框架按「有字段才查资源」的规则去找 json）。
     */
    @Test
    public void noObjectSelectionStaysFieldless() {
        List<String> formFields = new ArrayList<>();
        for (Field f : NoObjectSelection.class.getDeclaredFields()) {
            if (f.getAnnotation(FormField.class) != null) {
                formFields.add(f.getName());
            }
        }
        Assert.assertTrue("NoObjectSelection 应保持零 @FormField 字段，实际有：" + formFields,
                formFields.isEmpty());
    }

    // ===================================================================
    // 2. autoSelectActiveObject 的取值口径
    // ===================================================================

    /**
     * 字段初值是 {@code TRUE}，且 {@code null} 按<b>开启</b>处理。
     *
     * <p>{@code null} 那条不是理论情形：XStream 反序列化不走构造器、也不执行字段初值，
     * 本字段上线前落盘的 XML 里没有 {@code <autoSelectActiveObject>} 元素，
     * 反序列化出来的就是 {@code null}。此时按 false 处理会让存量 widget 静默失去
     * 「加载即选中第一行」的行为 —— 且对用户是完全无声的。
     */
    @Test
    public void autoSelectDefaultsToOn() {
        ObjectListWidget widget = new ObjectListWidget();
        Assert.assertEquals("字段初值应为 TRUE", Boolean.TRUE, widget.autoSelectActiveObject);
        Assert.assertTrue("字段初值时 resolve 也应为 true", widget.autoSelectActiveObject());

        widget.autoSelectActiveObject = null;
        Assert.assertTrue("存量 XML 缺该元素时字段是 null，语义应为「按默认开启」",
                widget.autoSelectActiveObject());

        widget.autoSelectActiveObject = Boolean.FALSE;
        Assert.assertFalse("显式关闭时必须真的关闭", widget.autoSelectActiveObject());

        widget.autoSelectActiveObject = Boolean.TRUE;
        Assert.assertTrue("显式开启", widget.autoSelectActiveObject());
    }

    // ===================================================================
    // 3. json 资源
    // ===================================================================

    /**
     * {@code dftVal} 必须是真布尔 {@code true}（不是字符串 "true"），且与 Java 初值一致。
     *
     * <p>前端新建表单的初值<b>只</b>来自 {@code eprops.dftVal}，Java 字段初始化器它一概不读
     * （见 {@code TestWorkshopFormDefaults} 的类注释）。写成字符串时控件不会勾上，
     * 于是「默认自动选中第一行」这个约定无声地失效了。
     */
    @Test
    public void jsonDeclaresTrueDftVal() throws Exception {
        JSONObject json = loadJson();
        JSONObject props = json.getJSONObject(ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT);
        Assert.assertNotNull("ObjectListWidget.json 缺 " + ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT
                + " —— 新建表单上该字段没有 label / help", props);

        Object dftVal = props.get("dftVal");
        Assert.assertNotNull("autoSelectActiveObject 缺 dftVal —— 新建 widget 默认不自动选中", dftVal);
        Assert.assertTrue("dftVal 必须是 JSON 布尔而不是字符串，实际：" + dftVal.getClass().getName(),
                dftVal instanceof Boolean);
        Assert.assertEquals("dftVal 必须与 Java 初值一致",
                new ObjectListWidget().autoSelectActiveObject, dftVal);

        // 字段类型：ENUM 是 Boolean 开关字段的既有写法（与 showObjectCount / hideNullProperties 一致）
        FormField formField = ObjectListWidget.class
                .getField(ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT).getAnnotation(FormField.class);
        Assert.assertNotNull("autoSelectActiveObject 必须是 @FormField", formField);
        Assert.assertEquals(FormFieldType.ENUM, formField.type());
        Assert.assertFalse("自动选中不该出现在「高级」分组里 —— 它是选择行为的常规配置",
                formField.advance());
    }

    /**
     * 宿主的两个单选字段都要有 json 条目（{@code activeObjectVar} 此前只有 help、没有 label）。
     *
     * <p>只断言「有条目」而不是逐字比 label 文案 —— 文案会随产品调整，钉死它只会制造无意义的摩擦。
     */
    @Test
    public void bothSingleSelectionFieldsHaveFormMetadata() throws IOException {
        JSONObject json = loadJson();
        Assert.assertNotNull("activeObjectVar 应有表单元数据",
                json.getJSONObject(ObjectListWidget.KEY_ACTIVE_OBJECT_VAR));
        Assert.assertNotNull("autoSelectActiveObject 应有表单元数据",
                json.getJSONObject(ObjectListWidget.KEY_AUTO_SELECT_ACTIVE_OBJECT));
    }

    // ===================================================================
    // 工具
    // ===================================================================

    private static Field fieldOrNull(Class<?> clazz, String name) {
        try {
            return clazz.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    private static JSONObject loadJson() throws IOException {
        try (InputStream in = ObjectListWidget.class.getResourceAsStream("ObjectListWidget.json")) {
            Assert.assertNotNull("同包下找不到 ObjectListWidget.json（框架按 SimpleName + .json 取资源）", in);
            return JSONObject.parseObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}

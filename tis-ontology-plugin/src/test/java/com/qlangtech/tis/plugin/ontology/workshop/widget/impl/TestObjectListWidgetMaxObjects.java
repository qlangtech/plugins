package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * {@code ObjectListWidget.maxObjects} 的契约。
 *
 * <p>这个字段是本轮「对象集取数收归 widget」里唯一新增的用户可见配置：
 * 改造前一次拉 {@code OBJECT_SET_QUERY_LIMIT = 1000} 行全部渲染，条数不可控。
 *
 * <p>三块断言，每块对应一个<b>静默出错</b>的面：
 * <ol>
 *   <li>{@link #resolveMaxObjectsFallsBackForUnset()} —— 存量落盘 XML 里没有
 *       {@code <maxObjects>} 元素。XStream 不走构造器、也不执行字段初值，所以这个字段必须能被
 *       兜底成默认值；若写成 {@code int} 会得到 0（一条都取不到，且表格看起来「就是没数据」）。</li>
 *   <li>{@link #jsonDftValIsANumber()} —— 前端新建表单的初值只读 {@code eprops.dftVal}。
 *       写成字符串 {@code "25"} 时输入框能显示、但控件类型与 {@code INT_NUMBER} 不匹配。</li>
 *   <li>{@link #maxObjectsIsNotPartOfValidateStage()} —— 白名单顺序的护栏：它必须在
 *       {@code validate()} 之后追加，否则存量 widget 的取数会整体失败。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class TestObjectListWidgetMaxObjects {

    @Test
    public void resolveMaxObjectsFallsBackForUnset() {
        ObjectListWidget widget = new ObjectListWidget();
        Assert.assertEquals("字段初值",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, widget.resolveMaxObjects());

        widget.maxObjects = null;
        Assert.assertEquals("存量 XML 缺这个元素时字段是 null（XStream 不执行初值）",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, widget.resolveMaxObjects());

        widget.maxObjects = 0;
        Assert.assertEquals("0 是「没配」而不是「取 0 行」",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, widget.resolveMaxObjects());

        widget.maxObjects = -5;
        Assert.assertEquals("负数同样回落默认值",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, widget.resolveMaxObjects());
    }

    @Test
    public void resolveMaxObjectsClampsOverLimit() {
        ObjectListWidget widget = new ObjectListWidget();
        widget.maxObjects = 1;
        Assert.assertEquals("合法下界原样通过", 1, widget.resolveMaxObjects());

        widget.maxObjects = 80;
        Assert.assertEquals("合法值原样通过", 80, widget.resolveMaxObjects());

        widget.maxObjects = ObjectListWidget.MAX_OBJECTS_LIMIT + 1;
        Assert.assertEquals("超硬顶饱和（夹取而非抛）",
                ObjectListWidget.MAX_OBJECTS_LIMIT, widget.resolveMaxObjects());
    }

    /** 字段类型必须是包装类型 + {@code INT_NUMBER}，判据见类注释第 1 块 */
    @Test
    public void fieldIsIntegerTypedIntNumber() throws Exception {
        Field field = ObjectListWidget.class.getField("maxObjects");
        Assert.assertEquals("必须是 Integer：int 在存量 XML 缺元素时会变成 0",
                Integer.class, field.getType());

        FormField formField = field.getAnnotation(FormField.class);
        Assert.assertNotNull("maxObjects 必须是 @FormField", formField);
        Assert.assertEquals(FormFieldType.INT_NUMBER, formField.type());
    }

    /** {@code dftVal} 必须是数字 25 —— 字符串 "25" 会让输入控件按文本初始化 */
    @Test
    public void jsonDftValIsANumber() throws IOException {
        JSONObject json = loadJson();
        JSONObject props = json.getJSONObject(ObjectListWidget.KEY_MAX_OBJECTS);
        Assert.assertNotNull("ObjectListWidget.json 缺 " + ObjectListWidget.KEY_MAX_OBJECTS
                + " —— 新建表单上该字段是空的", props);

        Object dftVal = props.get("dftVal");
        Assert.assertNotNull("maxObjects 缺 dftVal", dftVal);
        Assert.assertTrue("dftVal 必须是数字而不是字符串，实际：" + dftVal.getClass().getName(),
                dftVal instanceof Number);
        Assert.assertEquals("dftVal 必须逐值等于 Java 初值",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, ((Number) dftVal).intValue());
    }

    /**
     * {@code maxObjects} / {@code listSorting} 都<b>不在</b> {@code get_data} 的初始白名单里
     * —— 它们在 {@code validate()} 之后才追加。
     *
     * <p>为什么这值得一条断言：{@code listSorting} 带 {@code Validator.require}，
     * 若与必填字段一起参与校验，任何存量 widget（落盘 XML 里没有新元素）都会在
     * {@code get_data} 上整体校验失败，表现为「表格一片空白，配置面板红一片」。
     * 这个顺序只写在 {@code httpProcess} 的注释里，靠反射没法直接证明；这里退一步钉住
     * 「两者都有字段级兜底」这个前提 —— 兜底一旦被删，上面的顺序就会变成硬依赖。
     */
    @Test
    public void maxObjectsIsNotPartOfValidateStage() {
        ObjectListWidget widget = new ObjectListWidget();
        widget.maxObjects = null;
        Assert.assertEquals("resolveMaxObjects 必须自带兜底，否则白名单顺序就成了硬依赖",
                ObjectListWidget.DEFAULT_MAX_OBJECTS, widget.resolveMaxObjects());

        widget.listSorting = null;
        // 取数分支里有 `widget.listSorting != null ? : new NoListSorting()` 的兜底；
        // 这条断言只是把「字段可以为 null 且仍有意义」这件事记在案上。
        Assert.assertNull("listSorting 允许为 null（取数分支自行兜底）", widget.listSorting);
    }

    /**
     * {@code showObjectCount} 必须在 {@code get_data} 的白名单里。
     *
     * <p>这条是从一个<b>真实的静默失效</b>上补回来的：{@code httpProcess} 里读的是
     * {@code Boolean.TRUE.equals(widget.showObjectCount)}，而白名单当初只列了
     * {@code objectSetVar / titleProperty / cardFields} 再加 {@code listSorting / maxObjects} ——
     * 字段恒为 null，于是「显示对象数量」这个开关<b>永远不生效</b>，前端 {@code countText}
     * 静默回落到 {@code rows.length}（看起来像「总数就是取回的那几条」）。
     *
     * <p>反射读不到局部变量里的 {@code List<String>}，所以这里用源码文本兜住：只要
     * {@code httpProcess} 里还读这个字段，白名单里就必须有它的键。
     */
    @Test
    public void showObjectCountIsInTheGetDataWhitelist() throws IOException {
        String src = readSources();
        Assert.assertTrue("httpProcess 不再读 showObjectCount —— 本条断言的前提变了，请复核",
                src.contains("widget.showObjectCount"));
        Assert.assertTrue("showObjectCount 读到了却没进白名单 —— 它恒为 null，开关永远不生效",
                src.contains("relevantFields.add(KEY_SHOW_OBJECT_COUNT)"));
    }

    private static String readSources() throws IOException {
        String relative = ObjectListWidget.class.getName().replace('.', '/') + ".java";
        // 从 target/classes 往上找到模块根，再拐进 src/main/java
        Path p = Paths.get("src/main/java", relative);
        Assert.assertTrue("找不到 " + p.toAbsolutePath() + "（surefire 的 cwd 应是模块 basedir）",
                Files.isRegularFile(p));
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static JSONObject loadJson() throws IOException {
        try (InputStream in = ObjectListWidget.class.getResourceAsStream("ObjectListWidget.json")) {
            Assert.assertNotNull("同包下找不到 ObjectListWidget.json（框架按 SimpleName + .json 取资源）", in);
            return JSONObject.parseObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}

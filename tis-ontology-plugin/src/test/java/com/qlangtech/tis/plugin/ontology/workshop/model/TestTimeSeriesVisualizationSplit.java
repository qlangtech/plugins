package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code AbstractTimeSeriesVisualization} 多态化结构回归：抽象父类 + off / 启用两个子类。
 *
 * <p>原「是否显示可视化」是 {@code MetricConfig.showVisualization} 布尔开关，且配置挂在每个
 * 指标上；本类的可视化只属于卡片布局。结构漂移同样是静默失效（下拉少项 / ordinal 撞号 /
 * 默认值变成过不了校验的空壳），逐条钉死。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class TestTimeSeriesVisualizationSplit {

    private static final String PKG = "com.qlangtech.tis.plugin.ontology.workshop.model";

    private static final Comparator<Class<?>> BY_CLASS_NAME =
            Comparator.comparing((Class<?> c) -> c.getName());

    private static final Set<Class<?>> EXPECTED_SUBCLASSES = expectedSubclasses();

    private static Set<Class<?>> expectedSubclasses() {
        Set<Class<?>> result = new TreeSet<>(BY_CLASS_NAME);
        result.add(TimeSeriesVisualization.class);
        result.add(TimeSeriesVisualizationOff.class);
        return result;
    }

    @Test
    public void concreteSubclassesAreExactlyOffAndEnabled() {
        Set<Class<?>> actual = new TreeSet<>(BY_CLASS_NAME);
        actual.addAll(TisPluginFormTestSupport.concreteSubclassesOf(
                AbstractTimeSeriesVisualization.class, PKG));
        Assert.assertEquals("AbstractTimeSeriesVisualization 的具体子类应恰好是 off / 启用实现",
                EXPECTED_SUBCLASSES, actual);
    }

    @Test
    public void parentAndOffSubclassDeclareNoFormFields() {
        Assert.assertTrue("AbstractTimeSeriesVisualization 应是抽象类",
                java.lang.reflect.Modifier.isAbstract(AbstractTimeSeriesVisualization.class.getModifiers()));
        Assert.assertEquals("抽象父类不得声明 @FormField —— 字段全在子类，"
                        + "否则与子类 ordinal 撞号",
                0, formFieldsOf(AbstractTimeSeriesVisualization.class).size());

        Assert.assertEquals("off 子类应是零字段（「不显示」不需要任何参数）",
                0, formFieldsOf(TimeSeriesVisualizationOff.class).size());
        Assert.assertNull("零字段的 off 子类不应有 .json",
                TisPluginFormTestSupport.formResource(TimeSeriesVisualizationOff.class.getName()));
    }

    @Test
    public void enabledSubclassFieldsAreRequiredAndDescribed() {
        Map<String, FormField> fields = formFieldsOf(TimeSeriesVisualization.class);
        Assert.assertEquals("启用实现应恰有 position / timeSeriesSet / timeRange / showBaseline 四个表单字段",
                new TreeSet<>(Arrays.asList("position", "timeSeriesSet", "timeRange", "showBaseline")),
                new TreeSet<>(fields.keySet()));

        Assert.assertTrue(TimeSeriesVisualization.class.getSimpleName() + ".timeSeriesSet 必须带 Validator.require："
                        + "多态化后「选启用」就是显式启用动作，此时不能留空",
                Arrays.asList(fields.get("timeSeriesSet").validate()).contains(Validator.require));

        JSONObject resource = TisPluginFormTestSupport.formResource(TimeSeriesVisualization.class.getName());
        Assert.assertNotNull("启用实现声明了 @FormField，必须有 TimeSeriesVisualization.json", resource);
        for (String name : fields.keySet()) {
            Assert.assertNotNull("TimeSeriesVisualization.json 缺少字段 " + name + " 的条目",
                    resource.getJSONObject(name));
        }
    }

    @Test
    public void descriptorsHaveDistinctIdsAndExpectedDisplayNames() {
        Descriptor<?> off = TisPluginFormTestSupport.descriptorOf(TimeSeriesVisualizationOff.class);
        Assert.assertEquals("off 子类的 displayName 必须是 Descriptor.SWITCH_OFF（\"off\"）",
                Descriptor.SWITCH_OFF, off.getDisplayName());
        Assert.assertEquals("off 子类的 descriptor id 应是自身类 FQCN",
                TimeSeriesVisualizationOff.class.getName(), off.getId());
        Assert.assertEquals("off descriptor 的泛型实参应是抽象父类",
                AbstractTimeSeriesVisualization.class, off.getT());

        Descriptor<?> enabled = TisPluginFormTestSupport.descriptorOf(TimeSeriesVisualization.class);
        Assert.assertEquals("启用实现的 displayName 应保持 \"Time Series Visualization\"",
                "Time Series Visualization", enabled.getDisplayName());
        Assert.assertEquals("启用实现的 descriptor id 应是自身类 FQCN",
                TimeSeriesVisualization.class.getName(), enabled.getId());
        Assert.assertEquals("启用实现 descriptor 的泛型实参也应是抽象父类",
                AbstractTimeSeriesVisualization.class, enabled.getT());

        Assert.assertNotEquals("两个子类的 descriptor id 不得相同（descriptor 被提到父类会撞号）",
                off.getId(), enabled.getId());
    }

    /**
     * 宿主是 {@code CardLayout}（不是 MetricConfig）：可视化只属于卡片布局。
     */
    @Test
    public void hostIsCardLayoutAndDefaultsOff() throws Exception {
        Field field = CardLayout.class.getField("visualization");
        Assert.assertEquals("CardLayout.visualization 的声明类型应为抽象父类",
                AbstractTimeSeriesVisualization.class, field.getType());
        Assert.assertTrue("CardLayout.visualization 的 Java 初值应为 off 子类",
                field.get(new CardLayout()) instanceof TimeSeriesVisualizationOff);
        Assert.assertTrue("宿主字段应带 Validator.require（多态化后它恒非空）",
                Arrays.asList(field.getAnnotation(FormField.class).validate()).contains(Validator.require));

        JSONObject resource = TisPluginFormTestSupport.formResource(CardLayout.class.getName());
        Assert.assertNotNull("CardLayout.json 应存在", resource);
        JSONObject visualization = resource.getJSONObject("visualization");
        Assert.assertNotNull("CardLayout.json 应有 visualization 条目", visualization);
        Assert.assertEquals("CardLayout.visualization 的默认值应为 off（与 Validator.require 配套）",
                Descriptor.SWITCH_OFF, visualization.getString("dftVal"));

        // TagLayout / ListLayout 不得携带可视化（否则又变成「永不渲染的字段」）
        for (Class<?> layout : Arrays.asList(TagLayout.class, ListLayout.class, LayoutStyle.class)) {
            try {
                layout.getField("visualization");
                Assert.fail(layout.getSimpleName() + " 不应有 visualization 字段 —— 可视化只属于卡片布局");
            } catch (NoSuchFieldException expected) {
                // ok
            }
        }
    }

    /** 本类自身或祖先链上声明的 {@code @FormField} 字段（按字段名索引） */
    private static Map<String, FormField> formFieldsOf(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields.stream()
                .filter(f -> f.getAnnotation(FormField.class) != null)
                .collect(Collectors.toMap(Field::getName, f -> f.getAnnotation(FormField.class)));
    }
}

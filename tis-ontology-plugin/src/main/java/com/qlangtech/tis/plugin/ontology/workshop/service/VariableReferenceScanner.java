package com.qlangtech.tis.plugin.ontology.workshop.service;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 变量引用扫描器 —— 删除变量前的防误删校验。
 *
 * <p>模块内的变量引用一律存的是<strong>变量名</strong>（大小写不敏感，见
 * {@code ConditionalVisibility.variableId} 的 javadoc），散落在两类宿主上：
 * <ul>
 *   <li>布局树：{@code module.pages[].sections[].widgets[]} / {@code module.overlays[].sections[]}
 *       / {@code module.headerWidgets[]} / {@code module.routingConfig} / {@code module.autoRefreshConfig}
 *       / {@code module.header}，以及 widget 内嵌的各类配置对象
 *       （{@code ConditionalVisibility.variableId}、{@code ChartLayer.selectionAsFilterVar}、
 *       {@code LoopLayout.iteratorVariable} …）；</li>
 *   <li>其余变量自身的配置子树：{@code ObjectSetDefinitionConfig.filterVar}（对象集变量的过滤条件
 *       引用别的变量）、{@code FunctionParamConfig.variableId}、{@code InterfaceInputRow.variableName} …。</li>
 * </ul>
 *
 * <p>扫描不做逐字段白名单 —— 那样每加一个绑定字段就要同步改这里，漏改即静默漏报。
 * 这里反其道行之：遍历整棵对象图取值判定，命中谓词只有一条——「字段名长得像变量绑定
 * （{@code *Var} 结尾，或含 {@code variable}），且字段值与被删变量名忽略大小写相等」。
 * 遍历分三层（见 {@link #walk}）：<b>表单属性元数据优先</b>——对齐
 * {@code RootFormProperties#getInstancePropsJson} 的范式，按 descriptor 声明的
 * {@code PropertyType} 取值下钻，「声明即契约」，static/synthetic/回指字段天然不会混进来；
 * <b>transient 聚合字段补扫</b>——{@code page.sections} / {@code section.widgets} /
 * {@code module.variables} 是 readResolve 回填的 transient 字段，不是 {@code @FormField}，
 * 元数据里根本看不到；<b>裸反射兜底</b>——{@code getDescriptor()} 依赖 TIS 运行期
 * （纯单测环境不可用、插件被卸载返回 null、个别类型元数据构建失败），三种情况回落反射。
 * 代价是理论上可能纳入非绑定的同值字段，但布局树的字段命名约定（
 * {@code .../workshop/model} 与 {@code .../workshop/widget/impl} 下全部绑定字段都遵守
 * {@code *Var} / {@code *variable*} 命名）使得这类假阳性在现状代码里不存在。
 *
 * <h3>刻意排除的字段</h3>
 * <ul>
 *   <li>{@code LoopLayout.itemVariable}：「迭代项变量名」是循环<em>声明</em>的合成变量，
 *       不是对既有变量的引用——删掉同名变量后循环仍自给自足地提供绑定，不构成删除障碍
 *       （{@code iteratorVariable} 则是真消费：被迭代的对象集变量，照常纳入）；</li>
 *   <li>{@code parent}（{@code DescribableJSON} 的祖先链）、{@code formItem}：回指/派生字段，
 *       下钻只会产出与主路径重复的命中；</li>
 *   <li>{@code Descriptor} 及 JDK 内部类型：元数据图与实例配置无关。</li>
 * </ul>
 *
 * <p>引用绑定是<strong>模块内</strong>的：widget 表单的变量候选来自
 * {@code WidgetOptionHelper.getVariableOptions}，只列当前模块的变量，因此扫描范围就是
 * 被删变量所在的模块 —— 跨模块扫描反而会把「两个模块各有一个同名变量」误判成引用。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/16
 */
final class VariableReferenceScanner {

    /**
     * 对象图下钻深度上限：布局树的实际深度远小于此值，只防未知结构的环
     * （{@link #visited} 已按对象身份防环，这里防的是超长链路）
     */
    private static final int MAX_DEPTH = 30;

    /**
     * 引用清单条数上限，防止极端情况下错误消息无限膨胀
     */
    private static final int MAX_HITS = 100;

    /**
     * 跳过的字段：回指/派生字段，下钻只会产出重复命中
     */
    private static final Set<String> SKIP_FIELDS = Set.of("parent", "formItem");

    /**
     * {@link WorkshopModule} 上已由 {@code scan} 带可读标签单独下钻的根
     * （全部是 transient 回填字段，反正也进不了 {@code getFields()} 循环，这里防的是
     * 未来有字段去掉 transient 后被重复扫描）
     */
    private static final Set<String> MODULE_ROOTS_HANDLED_SEPARATELY =
            Set.of("pages", "overlays", "headerWidgets", "variables", "header");

    private static final Map<Class<?>, List<Field>> FIELDS_CACHE = new ConcurrentHashMap<>();

    private final String variableName;
    private final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<String> hits = new ArrayList<>();
    private boolean truncated = false;

    private VariableReferenceScanner(String variableName) {
        this.variableName = variableName;
    }

    /**
     * 扫描模块布局树与其余变量的配置子树，找出所有引用了目标变量的位置。
     *
     * @param module          已回填（pages / overlays / headerWidgets / variables 均就位）的模块
     * @param rawVariableName 被删变量的名字（大小写随意，内部会归一化）
     * @return 引用位置描述列表（形如
     * {@code 页面「首页」> 分区「销售」> 组件「KPI 表格」 : objectSetVar}），空列表 = 无引用
     */
    static List<String> scan(WorkshopModule module, String rawVariableName) {
        if (module == null) {
            return Collections.emptyList();
        }
        String victim = WorkshopVariable.normalizeName(rawVariableName);
        if (StringUtils.isEmpty(victim)) {
            throw new IllegalArgumentException("param variableName can not be empty");
        }
        VariableReferenceScanner scanner = new VariableReferenceScanner(victim);

        for (WorkshopPage page : safeList(module.pages)) {
            scanner.walk(page, "页面「" + firstNonBlank(page == null ? null : page.displayName,
                    page == null ? null : page.name, page == null ? null : page.id) + "」", 0);
        }
        for (WorkshopOverlay overlay : safeList(module.overlays)) {
            scanner.walk(overlay, "浮层「" + firstNonBlank(overlay == null ? null : overlay.name,
                    overlay == null ? null : overlay.id) + "」", 0);
        }
        for (WorkshopWidget widget : safeList(module.headerWidgets)) {
            scanner.walk(widget, "页头 > " + widgetLabel(widget), 0);
        }
        // header 也是 transient（readResolve 回填），必须显式下钻
        scanner.walk(module.header, "页头", 0);
        // routingConfig / autoRefreshConfig 等挂在模块上的非 transient 配置对象，
        // 字段名本身就是路径上下文
        for (Field field : WorkshopModule.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())
                    || MODULE_ROOTS_HANDLED_SEPARATELY.contains(field.getName())) {
                // 后者已在上面带可读标签单独处理，这里重复走只会产出难看的路径
                continue;
            }
            try {
                scanner.walk(field.get(module), field.getName(), 0);
            } catch (IllegalAccessException e) {
                continue;
            }
        }
        for (WorkshopVariable variable : safeList(module.variables)) {
            if (variable == null
                    || victim.equals(WorkshopVariable.normalizeName(variable.getName()))) {
                // 被删变量自己不算引用自己
                continue;
            }
            scanner.walk(variable, "变量「" + variable.getName() + "」", 0);
        }

        List<String> result = scanner.hits;
        if (scanner.truncated) {
            result.add("…（引用位置过多，仅列出前 " + MAX_HITS + " 处）");
        }
        return result;
    }

    // ==================================================================
    //  对象图遍历
    // ==================================================================

    private void walk(Object node, String path, int depth) {
        if (node == null || depth > MAX_DEPTH) {
            return;
        }
        if (hits.size() >= MAX_HITS) {
            truncated = true;
            return;
        }
        // 叶子类型：值引用只在 String 字段上判定，其余叶子直接返回
        if (node instanceof String || node instanceof Number || node instanceof Boolean
                || node instanceof Character || node instanceof Enum || node instanceof Class) {
            return;
        }
        if (node instanceof Descriptor || !visited.add(node)) {
            // Descriptor 是插件元数据图，与实例配置无关；visited 按对象身份防环
            return;
        }

        if (node instanceof Map<?, ?> map) {
            for (Object val : map.values()) {
                walk(val, path, depth + 1);
            }
            return;
        }
        if (node instanceof Iterable<?> items) {
            int idx = 0;
            for (Object item : items) {
                walk(item, path + elementLabel(item, idx++), depth + 1);
            }
            return;
        }
        if (node.getClass().isArray()) {
            Object[] items = (Object[]) node;
            for (int idx = 0; idx < items.length; idx++) {
                walk(items[idx], path + elementLabel(items[idx], idx), depth + 1);
            }
            return;
        }
        Class<?> clazz = node.getClass();
        if (isFrameworkClass(clazz)) {
            return;
        }
        if (node instanceof Describable<?> plugin) {
            Descriptor<?> desc = descriptorOf(plugin);
            if (desc != null) {
                Map<String, IPropertyType> props = null;
                try {
                    props = desc.getPropertyTypes(true);
                } catch (Throwable e) {
                    // getPropertyTypes 的构建期校验（如 identity 契约）失败：整体回落反射，
                    // 宁可绕远也不漏报
                }
                if (props != null) {
                    walkByProperties(plugin, props, path, depth);
                    walkTransientFields(clazz, node, path, depth);
                    return;
                }
            }
        }
        walkByReflection(clazz, node, path, depth);
    }

    /**
     * 表单属性遍历：按 descriptor 声明的 {@link PropertyType} 逐项取值下钻，对齐
     * {@code RootFormProperties#getInstancePropsJson} 的范式——不必再自己过滤
     * static / synthetic / 回指字段。{@code getVal} 必须传 {@code serialize2Frontend=false}
     * （裸值）：{@code true} 会走前端序列化（ENUM 实例转 name、MULTI_SELECTABLE 转视图形态），
     * 引用比对要的是原始 String。List 型属性的值原样交给 {@link #visitValue}：容器继承路径
     * 下钻，元素若是 Describable 自然继续走元数据路径（{@code ChartLayer.dataSourceVar}
     * 就是这样覆盖到的），无需为 List / @SubForm 单开分支。
     */
    private void walkByProperties(Object plugin, Map<String, IPropertyType> props, String path, int depth) {
        for (Map.Entry<String, IPropertyType> entry : props.entrySet()) {
            if (!(entry.getValue() instanceof PropertyType propertyType)) {
                // 非 PropertyType 条目（@SubForm 的 SuFormProperties 等）：workshop 未用到，保守跳过
                continue;
            }
            String fieldName = propertyType.propertyName();
            if (SKIP_FIELDS.contains(fieldName)) {
                continue;
            }
            Object val;
            try {
                val = propertyType.getVal(false, plugin);
            } catch (Throwable e) {
                // 单属性取值失败只跳过该属性，不拖垮整趟扫描
                continue;
            }
            visitValue(fieldName, val, path, depth);
        }
    }

    /**
     * transient 聚合字段补扫：{@code page.sections} / {@code section.widgets} /
     * {@code module.variables} 这类由 readResolve 回填的聚合字段是 transient 的，
     * 不进 {@code @FormField}，属性元数据里看不到，必须反射补扫。
     * 只扫 transient，与元数据的覆盖面天然不相交，同一字段不会被两种遍历重复命中。
     */
    private void walkTransientFields(Class<?> clazz, Object node, String path, int depth) {
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : declaredFields(c)) {
                int mods = field.getModifiers();
                if (!Modifier.isTransient(mods) || Modifier.isStatic(mods)
                        || field.isSynthetic() || field.getName().startsWith("this$")
                        || SKIP_FIELDS.contains(field.getName())) {
                    continue;
                }
                Object val;
                try {
                    field.setAccessible(true);
                    val = field.get(node);
                } catch (Throwable e) {
                    continue;
                }
                visitValue(field.getName(), val, path, depth);
            }
        }
    }

    /**
     * 裸反射兜底：{@link #descriptorOf} 返回 null（TIS 未初始化的纯单测环境、插件被卸载）
     * 或属性元数据构建失败时，退回遍历全部实例字段。
     */
    private void walkByReflection(Class<?> clazz, Object node, String path, int depth) {
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : declaredFields(c)) {
                int mods = field.getModifiers();
                if (Modifier.isStatic(mods) || field.isSynthetic() || field.getName().startsWith("this$")
                        || SKIP_FIELDS.contains(field.getName())) {
                    continue;
                }
                Object val;
                try {
                    field.setAccessible(true);
                    val = field.get(node);
                } catch (Throwable e) {
                    // 同一 classloader（unnamed module）下的字段不会走到这里；保守跳过
                    continue;
                }
                visitValue(field.getName(), val, path, depth);
            }
        }
    }

    /**
     * 取值消费端（元数据遍历与反射兜底共用）：命中谓词只在这一个地方判定。
     * 路径规则——String 是叶子，命中记 {@code path : 字段名}；容器继承当前路径；
     * 其余对象（含单个嵌套 Describable）以 {@code path > 字段名} 下钻。
     */
    private void visitValue(String fieldName, Object val, String path, int depth) {
        if (val == null) {
            return;
        }
        if (val instanceof String strVal) {
            if (isReferenceField(fieldName)
                    && variableName.equals(WorkshopVariable.normalizeName(strVal))) {
                hits.add(path + " : " + fieldName);
            }
        } else if (val instanceof Map || val instanceof Iterable || val.getClass().isArray()) {
            walk(val, path, depth + 1);
        } else {
            walk(val, path + " > " + fieldName, depth + 1);
        }
    }

    /**
     * 取 Describable 的 descriptor，不可用时返回 null（触发反射兜底）。两种触发场景：
     * {@code TIS.get()} 未初始化（纯单测环境）时 default 实现直接 NPE；插件被卸载时
     * {@code TIS.getDescriptor(clazz)} 返回 null。
     */
    private static Descriptor<?> descriptorOf(Describable<?> plugin) {
        try {
            return plugin.getDescriptor();
        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * 引用字段的判定谓词：{@code *Var} 结尾（{@code objectSetVar} / {@code activeObjectVar} /
     * {@code filterVar} / {@code selectionAsFilterVar} …），或字段名含 {@code variable}
     * （{@code variableId} / {@code valueVariable} / {@code variableName} / {@code iteratorVariable} …）。
     * <p>{@code itemVariable} 刻意排除 —— 见类 javadoc「刻意排除的字段」。
     */
    private static boolean isReferenceField(String fieldName) {
        if ("itemVariable".equals(fieldName)) {
            return false;
        }
        if (fieldName.endsWith("Var")) {
            return true;
        }
        return fieldName.toLowerCase().contains("variable");
    }

    // ==================================================================
    //  路径标签
    // ==================================================================

    /**
     * 列表 / 数组元素的路径段：布局树里的知名实体给可读标签，其余给下标
     */
    private static String elementLabel(Object item, int index) {
        if (item instanceof WorkshopWidget) {
            return " > " + widgetLabel((WorkshopWidget) item);
        }
        if (item instanceof WorkshopSection) {
            WorkshopSection section = (WorkshopSection) item;
            return " > 分区「" + firstNonBlank(section.name, section.id) + "」";
        }
        if (item instanceof WorkshopVariable) {
            return " > 变量「" + ((WorkshopVariable) item).getName() + "」";
        }
        return "[" + index + "]";
    }

    private static String widgetLabel(WorkshopWidget widget) {
        if (widget == null) {
            return "组件";
        }
        if (StringUtils.isNotBlank(widget.title)) {
            return "组件「" + widget.title + "」";
        }
        String simpleName = widget.getClass().getSimpleName();
        return "组件「" + (simpleName.endsWith("Widget")
                ? simpleName.substring(0, simpleName.length() - "Widget".length()) : simpleName) + "」";
    }

    private static String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (StringUtils.isNotBlank(candidate)) {
                return candidate;
            }
        }
        return "?";
    }

    private static <T> List<T> safeList(List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    private static boolean isFrameworkClass(Class<?> clazz) {
        String pkg = clazz.getPackageName();
        return pkg.startsWith("java.") || pkg.startsWith("javax.") || pkg.startsWith("jdk.")
                || pkg.startsWith("sun.") || pkg.startsWith("com.sun.");
    }

    private static List<Field> declaredFields(Class<?> clazz) {
        return FIELDS_CACHE.computeIfAbsent(clazz, key -> {
            List<Field> fields = new ArrayList<>();
            for (Field field : key.getDeclaredFields()) {
                fields.add(field);
            }
            return Collections.unmodifiableList(fields);
        });
    }
}

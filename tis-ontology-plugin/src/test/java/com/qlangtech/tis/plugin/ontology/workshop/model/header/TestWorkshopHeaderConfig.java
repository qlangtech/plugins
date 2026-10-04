package com.qlangtech.tis.plugin.ontology.workshop.model.header;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.qlangtech.tis.extension.Descriptor.KEY_DESC_VAL;
import static com.qlangtech.tis.extension.Descriptor.KEY_primaryVal;

/**
 * {@code WorkshopHeader} 两个多态字段改造的接线回归测试。
 *
 * <p>本轮把 {@code orientation}（枚举 + {@code height}/{@code width} 两个平铺字段）与
 * 折叠三项（{@code collapsible} + {@code collapsedByDefault} + {@code collapsedImage}）
 * 改成「抽象基类 + 子类」，取值由 <b>Java 类型</b>承担。这类改造有一批<b>没有任何编译期
 * 保护</b>的接线，全部由本类钉住：
 *
 * <ol>
 *   <li><b>子类形态</b> —— 每个基类的具体子类集合双向相等；{@code CollapseConfig} 一族里
 *       恰好一个的 {@code getDisplayName()} 是 {@link Descriptor#SWITCH_OFF}</li>
 *   <li><b>descriptor 的 clazz</b> —— 每个子类的 {@code getId()} 必须等于它<b>自己</b>的
 *       FQCN。descriptor 一旦被提到基类上，两个子类 id 撞号，前端再也分辨不出 impl</li>
 *   <li><b>json 的存在性规则</b> —— 带 {@code @FormField} 的子类<b>必须</b>有自己的
 *       {@code .json}；零字段的关态子类<b>必须没有</b>（框架按约定只给「有字段」的类建）</li>
 *   <li><b>默认项接线（跨语言）</b> —— {@code WorkshopHeader.json} 里
 *       {@code orientation}/{@code collapseConfig} 的 {@code dftVal} 必须逐字等于对应
 *       子类 descriptor 的 {@code displayName}。前端 {@code tis.plugin.ts} 拿 {@code dftVal}
 *       去逐项比对 {@code displayName} 选默认项，Java 侧改个字符串默认项就悄悄丢了</li>
 *   <li><b>扁平字段不得复辟</b> —— {@code height}/{@code width}/{@code collapsible}/
 *       {@code collapsedByDefault}/{@code collapsedImage} 不允许再出现在 {@code WorkshopHeader} 上</li>
 *   <li><b>GET/POST 两种 JSON 形态的互逆关系</b> —— 见
 *       {@link #headerItemJsonAndPostBodyRoundTrip()}，需要 TIS 运行期，环境不具备时整条跳过</li>
 * </ol>
 *
 * <p>第 1~5 条<b>不依赖 TIS 运行期</b>（不需要 {@code --add-opens}）：类清单来自编译期生成的
 * sezpoz 索引，资源靠 classloader 读，descriptor 直接 {@code newInstance}。这样本类在默认
 * surefire 配置下就能真跑，不会因为环境问题整类变成「一直绿但什么都没查」。
 */
public class TestWorkshopHeaderConfig {

    /** 本轮改造所在的包 */
    private static final String HEADER_PKG = "com.qlangtech.tis.plugin.ontology.workshop.model.header.";

    private static final String FIELD_ORIENTATION = "orientation";
    private static final String FIELD_COLLAPSE_CONFIG = "collapseConfig";

    /** 期望的两个方向子类；本族<b>没有</b>关态（方向是必选维度，不存在「不设置方向」） */
    private static final Set<String> EXPECTED_ORIENTATIONS = new TreeSet<>(Arrays.asList(
            HorizontalOrientation.class.getSimpleName(), VerticalOrientation.class.getSimpleName()));

    /** 改造中被删除的扁平字段，任何一个复辟都说明改造被回退了 */
    private static final List<String> REMOVED_FLAT_FIELDS = Arrays.asList(
            "height", "width", "collapsible", "collapsedByDefault", "collapsedImage");

    // ===================================================================
    // 1. 子类形态
    // ===================================================================

    /**
     * {@code HeaderOrientation} 恰好两个子类，且<b>都不是</b>关态。
     *
     * <p>用双向相等而非「扫到的 ⊆ 期望」：后者在扫到 0 个时循环体不执行，会假通过。
     */
    @Test
    public void orientationHasExactlyTwoNonOffSubclasses() {
        Set<Class<?>> actual = TisPluginFormTestSupport.concreteSubclassesOf(
                HeaderOrientation.class, HEADER_PKG);
        Assert.assertEquals("HeaderOrientation 应恰好是两个具体子类（水平 / 垂直）",
                EXPECTED_ORIENTATIONS, TisPluginFormTestSupport.names(actual));

        for (Class<?> sub : actual) {
            Assert.assertNotEquals(sub.getSimpleName() + " 是方向子类，其 displayName 不得是 SWITCH_OFF"
                            + "（方向没有「不设置」这个状态）",
                    Descriptor.SWITCH_OFF, TisPluginFormTestSupport.descriptorOf(sub).getDisplayName());
        }
    }

    /**
     * {@code CollapseConfig} 恰好两个子类，其中恰好一个的 displayName 是
     * {@link Descriptor#SWITCH_OFF}。
     *
     * <p>判据刻意不是「类名里有 None」：前端选默认项比对的是 {@code displayName} 这个字符串，
     * 名字对了但 displayName 写错同样是坏的。
     */
    @Test
    public void collapseConfigHasExactlyOneOffSubclass() {
        Set<Class<?>> actual = TisPluginFormTestSupport.concreteSubclassesOf(
                CollapseConfig.class, HEADER_PKG);
        Assert.assertEquals("CollapseConfig 应恰好两个具体子类（可折叠 / 不可折叠）",
                new TreeSet<>(Arrays.asList(CollapsibleConfig.class.getSimpleName(),
                        NoneCollapseConfig.class.getSimpleName())),
                TisPluginFormTestSupport.names(actual));

        List<String> offs = new ArrayList<>();
        for (Class<?> sub : actual) {
            if (Descriptor.SWITCH_OFF.equals(TisPluginFormTestSupport.descriptorOf(sub).getDisplayName())) {
                offs.add(sub.getSimpleName());
            }
        }
        Assert.assertEquals("CollapseConfig 应恰好有一个子类的 displayName 是 SWITCH_OFF"
                + "（前端按它选默认项），实际是：" + offs, 1, offs.size());
        Assert.assertEquals("关态子类应是 NoneCollapseConfig",
                NoneCollapseConfig.class.getSimpleName(), offs.get(0));
    }

    /**
     * 四个子类 descriptor 的 {@code getId()} 都必须等于<b>它自己</b>的 FQCN。
     *
     * <p>{@code Descriptor()} 无参构造把 {@code clazz} 置为 {@code getClass().getEnclosingClass()}，
     * 所以只要 descriptor 嵌在具体子类里就自然成立；一旦有人图省事把 descriptor 提到基类上
     * （或写 {@code super(CollapseConfig.class)}），同族的两个子类 {@code getId()} 就撞成同一个，
     * 前端在 impl 下拉里再也分不出选了哪一个。
     */
    @Test
    public void descriptorsIdentifyTheirConcreteSubclass() {
        for (Class<?> sub : Arrays.asList(HorizontalOrientation.class, VerticalOrientation.class,
                CollapsibleConfig.class, NoneCollapseConfig.class)) {
            Assert.assertEquals(sub.getSimpleName() + " 的 descriptor.getId() 应等于其自身 FQCN"
                            + "（同族子类撞成同一个 id 时前端无法区分 impl）",
                    sub.getName(), TisPluginFormTestSupport.descriptorOf(sub).getId());
        }
    }

    // ===================================================================
    // 2. json 的存在性规则
    // ===================================================================

    /**
     * 声明了 {@code @FormField} 字段的子类<b>必须</b>有自己的 {@code .json}；
     * 零字段的关态子类<b>必须没有</b>。
     *
     * <p>两个方向都是静默失效：漏建 json 时字段的 label/help 全空（表单照常渲染）；
     * 给零字段的类建了 json 则说明有人往关态子类里加了字段 —— 那正是本次改造要消灭的
     * 「关着却带着载荷」的状态。
     */
    @Test
    public void formResourceExistenceFollowsFormFields() {
        for (Class<?> sub : Arrays.asList(HorizontalOrientation.class, VerticalOrientation.class,
                CollapsibleConfig.class, NoneCollapseConfig.class)) {
            boolean hasFormField = declaresFormField(sub);
            JSONObject resource = TisPluginFormTestSupport.formResource(sub.getName());
            if (hasFormField) {
                Assert.assertNotNull(sub.getSimpleName() + " 声明了 @FormField 字段，因此必须有同名 .json"
                        + "（缺失时表单里该字段的 label/help 全空，且没有任何报错）", resource);
                for (Field f : formFieldsOf(sub)) {
                    Assert.assertNotNull(sub.getSimpleName() + ".json 缺少字段 " + f.getName() + " 的条目",
                            resource.getJSONObject(f.getName()));
                }
            } else {
                Assert.assertNull(sub.getSimpleName() + " 是零字段的关态子类，不应有 .json"
                        + "（有 json 意味着有人往关态子类里加了字段）", resource);
            }
        }
    }

    // ===================================================================
    // 3. 默认项接线（跨语言约定）
    // ===================================================================

    /**
     * {@code WorkshopHeader.json} 里两个多态字段的 {@code dftVal} 必须逐字等于对应默认子类
     * descriptor 的 {@code displayName}。
     *
     * <p>这条没有任何编译期保护：前端拿 {@code dftVal} 去逐项比对子类的 {@code displayName}
     * 来选默认项，值写错的表现是「默认项悄悄拐到了另一个子类」，一行日志都没有。
     * 断言用 {@code descriptorOf(...).getDisplayName()} 而不是写死 {@code "Horizontal"}，
     * 这样改了 displayName 而忘了改 json 同样会被抓住（写成字面量就只能抓住一半）。
     */
    @Test
    public void polymorphicFieldsDefaultValueMatchesSubclassDisplayName() {
        JSONObject resource = TisPluginFormTestSupport.formResource(WorkshopHeader.class.getName());
        Assert.assertNotNull("WorkshopHeader.json 应存在", resource);

        assertDftValMatchesDisplayName(resource, FIELD_ORIENTATION, HorizontalOrientation.class);
        assertDftValMatchesDisplayName(resource, FIELD_COLLAPSE_CONFIG, NoneCollapseConfig.class);

        // 关态那个额外钉一次字面量：SWITCH_OFF 本身是框架常量，不是可以随便改的显示名
        Assert.assertEquals("collapseConfig 的默认项必须是框架的关态常量",
                Descriptor.SWITCH_OFF,
                resource.getJSONObject(FIELD_COLLAPSE_CONFIG).getString("dftVal"));
    }

    // ===================================================================
    // 4. 扁平字段不得复辟
    // ===================================================================

    /**
     * 改造中被删掉的五个扁平字段不允许再出现在 {@code WorkshopHeader} 上。
     *
     * <p>光靠注释说明不了问题：它们一旦被加回来（哪怕是为了「兼容旧数据」），
     * 「垂直方向 + height 有值」这种矛盾态就重新可表示了，而编译器不会有任何意见。
     */
    @Test
    public void removedFlatFieldsMustNotComeBack() {
        for (String fieldName : REMOVED_FLAT_FIELDS) {
            try {
                WorkshopHeader.class.getField(fieldName);
                Assert.fail("WorkshopHeader." + fieldName + " 已由多态子类承担，不得复辟"
                        + "（复辟即意味着自相矛盾的数据重新可表示）");
            } catch (NoSuchFieldException expected) {
                // 正是期望的结果
            }
        }

        // 两个多态字段的类型也一并钉住：必须是抽象基类，不能退回枚举或 Object
        assertFieldType(FIELD_ORIENTATION, HeaderOrientation.class);
        assertFieldType(FIELD_COLLAPSE_CONFIG, CollapseConfig.class);
    }

    // ===================================================================
    // 5. GET / POST 两种 JSON 形态的互逆关系
    // ===================================================================

    /**
     * {@code getItemJson()} 的产物（GET 下发给前端）与 {@code AttrValMap} 期望的提交体
     * （POST 回传后端）是<b>互逆</b>的两层包装，前端 {@code toPostBody()} 就是那个转换器。
     *
     * <p>这条是本次改造最不显眼、也最容易写错的地方：两层 JSON 形状不同而名字一样，
     * 静态读代码看不出差别。
     * <ul>
     *   <li>GET：{@code {impl, displayName, vals:{orientation:{impl, vals:{width:240}}}}}
     *       —— 嵌套描述符直接是 {@code {impl, vals}}，叶子是裸值</li>
     *   <li>POST：叶子包 {@code {_primaryVal: v}}、嵌套描述符包 {@code {descVal: {impl, vals}}}。
     *       依据不是猜的：{@code AttrVals.parseAttrValMap} 对非 JSON 的裸值会
     *       {@code RuntimeException}（只有 String 才被自动包一层），
     *       {@code AttrVals.isPluginEqual} 又硬性要求嵌套字段带 {@code descVal} 键 ——
     *       也就是说回传时不这样包，框架在处理「是否更新」时就取不到值</li>
     * </ul>
     *
     * <p>需要 TIS 运行期（{@code getDescriptor()} / {@code TIS.get().getDescriptor(impl)}），
     * 环境不具备时整条跳过；用 {@code -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"}
     * 可以本地跑起来。
     */
    @Test
    public void headerItemJsonAndPostBodyRoundTrip() {
        org.junit.Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过 GET/POST 线格式检查",
                tisHeaderDescriptorReachable());

        WorkshopHeader header = sampleHeader();

        // ---------- GET 侧：下发给前端的形状 ----------
        JSONObject item = toJSON(header);
        Assert.assertEquals("header 自身的 impl 应是 WorkshopHeader 的 FQCN",
                WorkshopHeader.class.getName(), item.getString("impl"));

        JSONObject vals = Objects.requireNonNull(item.getJSONObject("vals"),
                "getItemJson() 必须产出 vals");
        for (String removed : REMOVED_FLAT_FIELDS) {
            Assert.assertFalse("扁平字段 " + removed + " 不应再出现在下发的 vals 里"
                    + "（它已由子类的 impl + vals 承担）", vals.containsKey(removed));
        }

        JSONObject orientation = Objects.requireNonNull(vals.getJSONObject(FIELD_ORIENTATION),
                "orientation 应以嵌套对象下发，而不是一个字符串");
        Assert.assertEquals("切到垂直方向后 impl 应是 VerticalOrientation",
                VerticalOrientation.class.getName(), orientation.getString("impl"));
        Assert.assertEquals("垂直方向的载荷是子类自己的 width",
                240, orientation.getJSONObject("vals").getIntValue("width"));

        JSONObject collapse = Objects.requireNonNull(vals.getJSONObject(FIELD_COLLAPSE_CONFIG),
                "collapseConfig 应以嵌套对象下发");
        Assert.assertEquals(CollapsibleConfig.class.getName(), collapse.getString("impl"));
        Assert.assertTrue("可折叠时的 collapsedByDefault 应在子类自己的 vals 里",
                collapse.getJSONObject("vals").getBooleanValue("collapsedByDefault"));
        Assert.assertEquals("折叠图标同理", "http://example.com/logo.png",
                collapse.getJSONObject("vals").getString("collapsedImage"));

        // ---------- POST 侧：前端回传的形状 ----------
        JSONObject postBody = toPostBody(item);
        Assert.assertTrue("POST 侧嵌套描述符必须包一层 descVal（否则 isPluginEqual 取不到值）",
                postBody.getJSONObject("vals").getJSONObject(FIELD_ORIENTATION).containsKey(KEY_DESC_VAL));
        Assert.assertTrue("POST 侧叶子值必须包一层 _primaryVal（否则 parseAttrValMap 直接抛错）",
                postBody.getJSONObject("vals").getJSONObject("title").containsKey(KEY_primaryVal));

        // parseDescribableMap 是真实入口：形状不对在这里就抛，走不到下面的断言
        AttrValMap attrValMap = AttrValMap.parseDescribableMap(Optional.empty(), postBody);
        Assert.assertEquals("根插件的 impl 应解析回 WorkshopHeader 的 descriptor",
                WorkshopHeader.class.getName(), attrValMap.descriptor.getId());

        // getPostJsonBody() 是框架自己的「可提交体」，把 POST 的两层包装再扒掉一层，
        // 应当回到 GET 侧的嵌套形状 —— 这正是两边互逆的证据
        JSONObject unwrapped = attrValMap.getPostJsonBody().getJSONObject("vals");
        JSONObject unwrappedOrientation = unwrapped.getJSONObject(FIELD_ORIENTATION);
        Assert.assertEquals(VerticalOrientation.class.getName(), unwrappedOrientation.getString("impl"));
        Assert.assertEquals("descVal 剥掉后 width 应还原成裸值",
                240, unwrappedOrientation.getJSONObject("vals").getIntValue("width"));
        Assert.assertEquals("_primaryVal 剥掉后 title 应还原成裸值",
                header.title, unwrapped.getString("title"));
        Assert.assertTrue(unwrapped.getJSONObject(FIELD_COLLAPSE_CONFIG)
                .getJSONObject("vals").getBooleanValue("collapsedByDefault"));
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    /** 一份「垂直方向 + 可折叠」的样本，两个多态字段都取<b>非默认</b>子类 */
    private static WorkshopHeader sampleHeader() {
        WorkshopHeader header = WorkshopHeader.defaultOf("salesDashboard");
        header.title = "销售看板";
        header.backgroundColor = "#1677ff";

        VerticalOrientation orientation = new VerticalOrientation();
        orientation.width = 240;
        header.orientation = orientation;

        CollapsibleConfig collapse = new CollapsibleConfig();
        collapse.collapsedByDefault = true;
        collapse.collapsedImage = "http://example.com/logo.png";
        header.collapseConfig = collapse;
        return header;
    }

    private static JSONObject toJSON(WorkshopHeader header) {
        try {
            return WorkshopHeader.toJSON(header);
        } catch (Exception e) {
            throw new AssertionError("WorkshopHeader.toJSON() 抛异常", e);
        }
    }

    /**
     * 把 GET 侧的形状转成 POST 侧的形状 —— 与前端
     * {@code workshop-module-api.service.ts} 的 {@code toPostBody()} 逐行同构。
     *
     * <p>放在测试里是刻意的：这样「前端以为的线格式」和「框架真正接受的线格式」在同一个
     * 用例里相遇，前端那份改错了这里的断言就会红。
     */
    private static JSONObject toPostBody(JSONObject itemJson) {
        JSONObject body = new JSONObject();
        body.put(AttrValMap.PLUGIN_EXTENSION_IMPL, itemJson.getString(AttrValMap.PLUGIN_EXTENSION_IMPL));

        JSONObject postVals = new JSONObject();
        JSONObject vals = itemJson.getJSONObject(AttrValMap.PLUGIN_EXTENSION_VALS);
        if (vals != null) {
            for (Map.Entry<String, Object> entry : vals.entrySet()) {
                Object val = entry.getValue();
                JSONObject wrapped = new JSONObject();
                if (val instanceof JSONObject && ((JSONObject) val).containsKey(AttrValMap.PLUGIN_EXTENSION_IMPL)) {
                    // 嵌套描述符：包 descVal
                    wrapped.put(KEY_DESC_VAL, toPostBody((JSONObject) val));
                } else {
                    // 叶子值：包 _primaryVal
                    wrapped.put(KEY_primaryVal, val);
                }
                postVals.put(entry.getKey(), wrapped);
            }
        }
        body.put(AttrValMap.PLUGIN_EXTENSION_VALS, postVals);
        return body;
    }

    private static void assertDftValMatchesDisplayName(JSONObject resource, String fieldName,
                                                       Class<?> defaultSubclass) {
        JSONObject fieldMeta = resource.getJSONObject(fieldName);
        Assert.assertNotNull(fieldName + " 在 WorkshopHeader.json 里应有条目", fieldMeta);
        Assert.assertEquals(fieldName + " 的 dftVal 必须逐字等于默认子类 "
                        + defaultSubclass.getSimpleName() + " 的 displayName"
                        + "（前端按它逐项比对子类 displayName 选默认项）",
                TisPluginFormTestSupport.descriptorOf(defaultSubclass).getDisplayName(),
                fieldMeta.getString("dftVal"));
    }

    private static void assertFieldType(String fieldName, Class<?> expectedType) {
        try {
            Assert.assertEquals("WorkshopHeader." + fieldName + " 的类型应是抽象基类",
                    expectedType, WorkshopHeader.class.getField(fieldName).getType());
        } catch (NoSuchFieldException e) {
            Assert.fail("WorkshopHeader 缺少 " + fieldName + " 字段");
        }
    }

    private static boolean declaresFormField(Class<?> clazz) {
        return !formFieldsOf(clazz).isEmpty();
    }

    private static List<Field> formFieldsOf(Class<?> clazz) {
        List<Field> result = new ArrayList<>();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.getAnnotation(FormField.class) != null) {
                result.add(f);
            }
        }
        return result;
    }

    /**
     * TIS 的 descriptor 扫描是否可用。
     *
     * <p>{@code DescriptorExtensionList} 是惰性的：真正的扩展扫描发生在迭代上，
     * 所以必须在这个 try 里就把它物化，否则异常会漏到外面变成失败而非跳过。
     */
    private static boolean tisHeaderDescriptorReachable() {
        try {
            TIS tis = TIS.get();
            return tis != null && !tis.getDescriptorList(WorkshopHeader.class).isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }
}

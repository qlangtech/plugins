package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.impl.EnumFieldMode;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ontology.BasicOntologyPropertyTypeRef;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateTimeWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DecimalNumberWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.EnumWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.FileWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.IntNumberWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.MultiDescriblePluginWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.MultiSelectableWidget;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DefaultOntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ValueConstraint;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4Integer;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.OneOfValueEnum;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4Decimal;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4Integer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code ActionParameterMapper} 那张「本体属性 → Action 参数配置」矩阵的回归测试。
 *
 * <p>这个映射是<b>纯约定、零编译期保护</b>的：{@link OntologyType} 有 17 个常量、{@link ValueConstraint}
 * 有若干子类，改成什么样都能编译通过，坏掉的表现是「界面上那个控件选不到 / 选项没预填 / 必填态反了」——
 * 全部都要人点到那一步才发现。所以这里逐格钉住：
 *
 * <ol>
 *   <li><b>第一层（值约束）优先</b>：引用 {@code Enum4String} / {@code Enum4Integer} 的属性必须拿到
 *       {@link EnumWidget} 且候选项逐条搬自 {@code enumVals}；{@code Range4Integer} / {@code Range4Decimal}
 *       必须搬到 {@link IntNumberWidget} / {@link DecimalNumberWidget} 的上下界上（含 {@code Float}→{@code Double}）</li>
 *   <li><b>第二层（本体类型）全覆盖</b>：{@link OntologyType#values()} 驱动的期望表——每个常量对应的控件类都被
 *       写死在这里，新增常量时这张表不会自动跟上，正是要它红</li>
 *   <li><b>永不返回 null</b>：{@code widgetType} 带 {@code Validator.require}，留空等于让每个生成的参数都卡在
 *       表单校验上</li>
 *   <li><b>四条降级路径</b>：没引用值类型、名字是空串、名字查不到、值类型表整个为 null —— 一律回落、不抛</li>
 * </ol>
 *
 * <p>测试全部是**手工搭 fixture**（直接给 {@link ActionParameterMapper#create} 传一张名字→值类型的 Map），
 * 因此不需要 PluginStore、不需要 {@code IPluginContext}、也不需要 TIS 运行期，在默认 surefire 配置下就能真跑。
 */
public class TestActionParameterMapper {

    // ===================================================================
    // 1. 第一层：值约束 → 控件
    // ===================================================================

    /**
     * {@code Enum4String} 的每个 {@code enumVal} 都要变成一条候选项，且 {@code showStyle} 必须给上
     * （该字段带 {@code Validator.require}，不设就是一个提交不了的表单）。
     */
    @Test
    public void enumOfStringBecomesEnumWidgetCarryingEveryOption() {
        Enum4String enumConstraint = new Enum4String();
        enumConstraint.enumVals = List.of(oneOf("RED"), oneOf("GREEN"), oneOf("BLUE"));

        PrimitiveDataType dataType = mapToDataType(
                withValueTypeRef(prop("color", OntologyType.STRING), "colorEnum"),
                valueTypes("colorEnum", OntologyType.STRING, enumConstraint));

        EnumWidget widget = asEnumWidget(dataType);

        Set<String> expected = new TreeSet<>(Arrays.asList("RED", "GREEN", "BLUE"));
        Set<String> actual = new TreeSet<>();
        for (WidgetOptionConfig option : widget.options) {
            actual.add(option.val);
            Assert.assertEquals("候选项的显示文本应等于取值（OneOfValueEnum 只有值没有 label）",
                    option.val, option.label);
        }
        Assert.assertEquals("枚举约束的每个 enumVal 都应成为一条候选项", expected, actual);
        Assert.assertEquals("showStyle 带 Validator.require，必须给出默认值",
                EnumFieldMode.DEFAULT, widget.showStyle);
    }

    /**
     * {@code Enum4Integer} 与 {@code Enum4String} 走同一支 —— 两者只差在所属的
     * {@code specializedTypeEnds()}，与控件形态无关。单独钉一条是因为 instanceof 链里它是<b>另一个分支</b>，
     * 漏了它不会有任何编译期提示。
     */
    @Test
    public void enumOfIntegerAlsoBecomesEnumWidget() {
        Enum4Integer enumConstraint = new Enum4Integer();
        enumConstraint.enumVals = List.of(oneOf("1"), oneOf("2"));

        PrimitiveDataType dataType = mapToDataType(
                withValueTypeRef(prop("level", OntologyType.INTEGER), "levelEnum"),
                valueTypes("levelEnum", OntologyType.INTEGER, enumConstraint));

        EnumWidget widget = asEnumWidget(dataType);
        Assert.assertEquals(2, widget.options.size());
        Assert.assertEquals("1", widget.options.get(0).val);
        Assert.assertEquals("2", widget.options.get(1).val);
    }

    /**
     * 整数上下界逐个相等地搬到 {@link IntNumberWidget} 上。
     */
    @Test
    public void integerRangeBecomesIntNumberWidgetWithSameBounds() {
        Range4Integer range = new Range4Integer();
        range.min = 1;
        range.max = 99;

        PrimitiveDataType dataType = mapToDataType(
                withValueTypeRef(prop("age", OntologyType.INTEGER), "ageRange"),
                valueTypes("ageRange", OntologyType.INTEGER, range));

        Assert.assertTrue("Range4Integer 应产出 IntNumberWidget，实际是 "
                        + dataType.widgetType.getClass().getSimpleName(),
                dataType.widgetType instanceof IntNumberWidget);
        IntNumberWidget widget = (IntNumberWidget) dataType.widgetType;
        Assert.assertEquals(Integer.valueOf(1), widget.min);
        Assert.assertEquals(Integer.valueOf(99), widget.max);
    }

    /**
     * 小数的上下界要完成 {@code Float}→{@code Double} 的搬迁，且 {@code null} 安全（约束侧允许留空，
     * 直接 {@code .doubleValue()} 会 NPE）。
     */
    @Test
    public void decimalRangeConvertsFloatBoundsToDoubleAndToleratesNull() {
        Range4Decimal range = new Range4Decimal();
        range.min = 1.5f;
        range.max = 9.25f;

        DecimalNumberWidget widget = (DecimalNumberWidget) mapToDataType(
                withValueTypeRef(prop("rate", OntologyType.DOUBLE), "rateRange"),
                valueTypes("rateRange", OntologyType.DOUBLE, range)).widgetType;
        Assert.assertEquals(Double.valueOf(1.5d), widget.min);
        Assert.assertEquals(Double.valueOf(9.25d), widget.max);

        Range4Decimal halfFilled = new Range4Decimal();
        halfFilled.max = 3f;
        DecimalNumberWidget partial = (DecimalNumberWidget) mapToDataType(
                withValueTypeRef(prop("rate", OntologyType.DOUBLE), "rateRange"),
                valueTypes("rateRange", OntologyType.DOUBLE, halfFilled)).widgetType;
        Assert.assertNull("约束侧未给下界时不得抛 NPE，也不得凭空造一个 0", partial.min);
        Assert.assertEquals(Double.valueOf(3d), partial.max);
    }

    /**
     * <b>第一层命中就用第一层</b>：属性本体类型是 STRING，却挂了 {@code Enum4Integer} 约束，产出的仍是
     * {@link EnumWidget}。若不成立，说明实现改成了「先按类型兜底」或两层的判定顺序被调换 ——
     * 那会让所有引用值类型的属性都失去约束带来的具体形态。
     */
    @Test
    public void valueConstraintWinsOverOntologyType() {
        Enum4Integer enumConstraint = new Enum4Integer();
        enumConstraint.enumVals = List.of(oneOf("7"));

        PrimitiveDataType dataType = mapToDataType(
                withValueTypeRef(prop("anything", OntologyType.STRING), "oddEnum"),
                valueTypes("oddEnum", OntologyType.INTEGER, enumConstraint));

        asEnumWidget(dataType);
    }

    // ===================================================================
    // 2. 第二层：本体类型全覆盖
    // ===================================================================

    /**
     * {@link OntologyType} 的每个常量都必须落在一支明确的控件上，且与写死在这里的期望表逐格相等。
     *
     * <p>期望表<b>故意与实现里的 switch 重复</b>：断言「非空」只能证明没漏，断言「等于这个类」才能证明
     * 没串行（把 DATE 接成 DateTime、把 FLOAT 接成整数控件之类的静默错配）。
     */
    @Test
    public void everyOntologyTypeMapsToItsDesignedWidget() {
        Map<OntologyType, Class<? extends UserInputWidgetType>> expected = new LinkedHashMap<>();
        expected.put(OntologyType.STRING, InputTextWidget.class);
        expected.put(OntologyType.INTEGER, IntNumberWidget.class);
        expected.put(OntologyType.SHORT, IntNumberWidget.class);
        expected.put(OntologyType.DATE, DateWidget.class);
        expected.put(OntologyType.TIMESTAMP, DateTimeWidget.class);
        expected.put(OntologyType.BOOLEAN, InputTextWidget.class);
        expected.put(OntologyType.BYTE, FileWidget.class);
        expected.put(OntologyType.LONG, IntNumberWidget.class);
        expected.put(OntologyType.FLOAT, DecimalNumberWidget.class);
        expected.put(OntologyType.DOUBLE, DecimalNumberWidget.class);
        expected.put(OntologyType.DECIMAL, DecimalNumberWidget.class);
        expected.put(OntologyType.VECTOR, InputTextWidget.class);
        expected.put(OntologyType.ARRAY, MultiSelectableWidget.class);
        expected.put(OntologyType.STRUCT, MultiDescriblePluginWidget.class);
        expected.put(OntologyType.GEOPOINT, InputTextWidget.class);
        expected.put(OntologyType.GEOSHAPE, InputTextWidget.class);
        expected.put(OntologyType.MEDIA_REFERENCE, FileWidget.class);

        Set<OntologyType> uncovered = new TreeSet<>(Arrays.asList(OntologyType.values()));
        uncovered.removeAll(expected.keySet());
        Assert.assertTrue("以下本体类型没有出现在本测试的期望表里（新增常量时请一并补上）：" + uncovered,
                uncovered.isEmpty());

        for (Map.Entry<OntologyType, Class<? extends UserInputWidgetType>> entry : expected.entrySet()) {
            // 无值类型引用 —— 走的正是第二层
            PrimitiveDataType dataType = mapToDataType(prop("p", entry.getKey()), Map.of());
            Assert.assertNotNull(entry.getKey() + " 没有映射出任何控件"
                    + "（widgetType 带 Validator.require，留空等于每个生成的参数都卡在表单校验上）",
                    dataType.widgetType);
            Assert.assertEquals(entry.getKey() + " 应映射到 " + entry.getValue().getSimpleName(),
                    entry.getValue(), dataType.widgetType.getClass());
        }
    }

    // ===================================================================
    // 3. 必填态与标识
    // ===================================================================

    /**
     * {@code required} 恰好是 {@code nullable} 的反面；{@code nullable} 未设置（存量数据 / 手工构造）
     * 时按必填处理 —— 宁可我多要一个值，也不要一个「谁都以为是必填」的参数静默变成可空。
     */
    @Test
    public void requiredIsExactlyTheOppositeOfNullable() {
        OntologyProperty nullableProp = prop("a", OntologyType.STRING);
        nullableProp.nullable = Boolean.TRUE;
        Assert.assertFalse("nullable=true 的属性应是非必填",
                mapToDataType(nullableProp, Map.of()).required);

        OntologyProperty nonNullProp = prop("b", OntologyType.STRING);
        nonNullProp.nullable = Boolean.FALSE;
        Assert.assertTrue("nullable=false 的属性应是必填",
                mapToDataType(nonNullProp, Map.of()).required);

        OntologyProperty unsetProp = prop("c", OntologyType.STRING);
        unsetProp.nullable = null;
        Assert.assertTrue("nullable 未设置时应按必填处理",
                mapToDataType(unsetProp, Map.of()).required);
    }

    /**
     * 参数标识与显示名都取属性名，{@code hidden} 为 false（本次不做智能隐藏推导）。
     */
    @Test
    public void identityAndDisplayNameComeFromPropertyName() {
        ActionParameterConfig config = ActionParameterMapper.create(
                prop("orderAmount", OntologyType.DECIMAL), Map.of());
        Assert.assertEquals("orderAmount", config.parameterId);
        Assert.assertEquals("orderAmount", config.displayName);
        Assert.assertEquals(Boolean.FALSE, config.hidden);
        Assert.assertEquals("orderAmount", config.identityValue());
    }

    /**
     * 属性为 null 直接拒绝：调用方在遍历 {@code getCols()}，null 说明上游数据坏了，
     * 这时应当立刻炸在源头而不是产出一行没有 key 的配置。
     */
    @Test
    public void nullPropertyIsRejected() {
        try {
            ActionParameterMapper.create(null, Map.of());
            Assert.fail("属性为 null 时应抛 NPE");
        } catch (NullPointerException expected) {
            // 正是期望的结果
        }
    }

    // ===================================================================
    // 4. 降级路径
    // ===================================================================

    /**
     * 四条降级路径都必须「不抛异常、也不留空」：候选集构建失败会让整个 Action 详情页打不开，
     * 而留空 {@code widgetType} 则是每个参数都过不了校验。四条的期望结果一致 —— 按本体类型取默认控件。
     */
    @Test
    public void everyDegradationPathFallsBackToTheTypeDefault() {
        // (1) 压根没引用值类型
        assertFallsBackToInputText(prop("p1", OntologyType.STRING), Map.of(), "未引用值类型");

        // (2) 表单里枚举未选时会交出 Optional.of("")，空串一律当「没引用」处理
        assertFallsBackToInputText(withValueTypeRef(prop("p2", OntologyType.STRING), ""),
                Map.of(), "值类型名是空串");

        // (3) 名字在本域内查不到（值类型被删或跨域引用）
        assertFallsBackToInputText(withValueTypeRef(prop("p3", OntologyType.STRING), "deletedRef"),
                valueTypes("otherRef", OntologyType.STRING, null), "值类型名查不到");

        // (4) 值类型表整个为 null（调用方没传）
        assertFallsBackToInputText(withValueTypeRef(prop("p4", OntologyType.STRING), "anyRef"),
                null, "值类型表为 null");

        // (5) 值类型引用了、但该值类型上没配约束步骤
        assertFallsBackToInputText(withValueTypeRef(prop("p5", OntologyType.STRING), "noConstraint"),
                valueTypesWithoutConstraint("noConstraint", OntologyType.STRING), "值类型上没有约束");

        // (6) 枚举约束的 enumVals 为空 —— 产一个没有任何候选项的下拉等于产一个提交不了的表单
        Enum4String emptyEnum = new Enum4String();
        emptyEnum.enumVals = List.of();
        assertFallsBackToInputText(withValueTypeRef(prop("p6", OntologyType.STRING), "emptyEnum"),
                valueTypes("emptyEnum", OntologyType.STRING, emptyEnum), "enumVals 为空");

        // (7) enumVals 非空，但每一条的 enumVal 都是空白 —— 逐条搬过去就是逐条 Validator.require 失败
        Enum4String blankEnum = new Enum4String();
        blankEnum.enumVals = List.of(oneOf(""), oneOf("   "), oneOf(null));
        assertFallsBackToInputText(withValueTypeRef(prop("p7", OntologyType.STRING), "blankEnum"),
                valueTypes("blankEnum", OntologyType.STRING, blankEnum), "enumVals 全是空白值");
    }

    /**
     * 枚举约束里<b>混着</b>空白值时，只丢弃空白项、其余照常成为候选项。
     *
     * <p>这条与上一条是两种不同的处理（整表回落 vs 逐条过滤），不区分就会把「有一条坏数据」
     * 放大成「整个枚举控件没了」。
     */
    @Test
    public void blankOptionIsDroppedWithoutLosingTheGoodOnes() {
        Enum4String enumConstraint = new Enum4String();
        enumConstraint.enumVals = List.of(oneOf("RED"), oneOf("  "), oneOf("BLUE"));

        EnumWidget widget = asEnumWidget(mapToDataType(
                withValueTypeRef(prop("color", OntologyType.STRING), "colorEnum"),
                valueTypes("colorEnum", OntologyType.STRING, enumConstraint)));

        Assert.assertEquals("空白候选项应被丢弃，其余应保留", 2, widget.options.size());
        Assert.assertEquals("RED", widget.options.get(0).val);
        Assert.assertEquals("BLUE", widget.options.get(1).val);
    }

    /**
     * 引用了一个「存在但不是 {@link DefaultOntologyValueType}」的值类型时同样回落。
     *
     * <p>值类型是多步插件，本域内落盘的都会是 {@code DefaultOntologyValueType}；用别名的实现
     * （或将来新增的子类）不该让这里 ClassCastException。
     */
    @Test
    public void nonDefaultValueTypeImplementationFallsBackInsteadOfCasting() {
        Map<String, OntologyValueType> valueTypes = new LinkedHashMap<>();
        valueTypes.put("aliased", new AliasedValueType());
        assertFallsBackToInputText(withValueTypeRef(prop("p", OntologyType.STRING), "aliased"),
                valueTypes, "值类型不是 DefaultOntologyValueType");
    }

    // ===================================================================
    //  helpers
    // ===================================================================

    private static void assertFallsBackToInputText(OntologyProperty col,
                                                   Map<String, OntologyValueType> valueTypes,
                                                   String scenario) {
        PrimitiveDataType dataType = mapToDataType(col, valueTypes);
        Assert.assertNotNull(scenario + "：降级后仍不得留空 widgetType", dataType.widgetType);
        Assert.assertEquals(scenario + "：应回落成本体类型对应的默认控件",
                InputTextWidget.class, dataType.widgetType.getClass());
    }

    private static PrimitiveDataType mapToDataType(OntologyProperty col,
                                                   Map<String, OntologyValueType> valueTypes) {
        ActionParameterConfig config = ActionParameterMapper.create(col, valueTypes);
        Assert.assertTrue("dataType 应是 PrimitiveDataType（对象引用只由规则层产生）",
                config.dataType instanceof PrimitiveDataType);
        return (PrimitiveDataType) config.dataType;
    }

    private static EnumWidget asEnumWidget(PrimitiveDataType dataType) {
        Assert.assertTrue("应映射到 EnumWidget，实际是 " + dataType.widgetType.getClass().getName(),
                dataType.widgetType instanceof EnumWidget);
        EnumWidget widget = (EnumWidget) dataType.widgetType;
        Assert.assertNotNull("EnumWidget.options 带 Validator.require，不得为 null", widget.options);
        Assert.assertTrue("EnumWidget.options 带 Validator.require，不得为空", !widget.options.isEmpty());
        Assert.assertNotNull("EnumWidget.showStyle 带 Validator.require，不得为 null", widget.showStyle);
        return widget;
    }

    private static OntologyProperty prop(String name, OntologyType type) {
        return new DefaultOntologyProperty(name, false, true, name + " description", type);
    }

    /** 属性侧只有值类型的<b>名字</b>，这里模拟表单填出的那个引用 */
    private static OntologyProperty withValueTypeRef(OntologyProperty col, String valueTypeName) {
        BasicOntologyPropertyTypeRef typeRef
                = (BasicOntologyPropertyTypeRef) col.getPropertyTypeRef();
        typeRef.valueType = valueTypeName;
        return col;
    }

    private static OneOfValueEnum oneOf(String enumVal) {
        OneOfValueEnum val = new OneOfValueEnum();
        val.setEnumVal(enumVal);
        return val;
    }

    /**
     * 造一个落在 {@code name} 名下、带指定约束的值类型。
     *
     * <p>步骤顺序不能反：{@code getMeta()} 取 Step1、{@code getConstraintsStep()} 取 Step2，
     * 与 {@code DefaultOntologyValueType.DefaultDesc#getStepDescriptionList()} 的声明一致。
     */
    private static Map<String, OntologyValueType> valueTypes(String name, OntologyType type,
                                                             ValueConstraint constraint) {
        Map<String, OntologyValueType> result = new LinkedHashMap<>();
        result.put(name, valueType(name, type, constraint));
        return result;
    }

    /** 与 {@link #valueTypes} 同，但第二步不给约束实例 */
    private static Map<String, OntologyValueType> valueTypesWithoutConstraint(String name, OntologyType type) {
        Map<String, OntologyValueType> result = new LinkedHashMap<>();
        result.put(name, valueType(name, type, null));
        return result;
    }

    private static DefaultOntologyValueType valueType(String name, OntologyType type,
                                                      ValueConstraint constraint) {
        MetadataOfValueType meta = new MetadataOfValueType();
        meta.name = name;
        meta.description = name + " description";
        meta.type = type.getValue();

        ConstraintsOfValueType constraints = new ConstraintsOfValueType();
        constraints.constraint = constraint;

        DefaultOntologyValueType valueType = new DefaultOntologyValueType();
        valueType.setSteps(new OneStepOfMultiSteps[]{meta, constraints});
        Assert.assertEquals("fixture 自检：值类型应按名字被寻址", name, valueType.identityValue());
        return valueType;
    }

    /**
     * 非 {@code DefaultOntologyValueType} 的值类型：只为覆盖「拿到手的不是预期实现」这条分支。
     *
     * <p>四个方法都是父类/接口要求的最小实现：{@code identityValue()} 是 {@code IdentityName} 的，
     * {@code getMultiStepsSavedItems()} 是 {@code MultiStepsSupportHost} 的（父类实现要遍历已落盘的多步实例，
     * 本替身没有），{@code getMeta()} 是 {@code OntologyValueType} 自己的抽象方法，
     * {@code getLiteria()} 来自 {@code PluginLiteria}。
     */
    private static final class AliasedValueType extends OntologyValueType {

        @Override
        public String identityValue() {
            return "aliased";
        }

        @Override
        public IMetadataOfValueType getMeta() {
            return new MetadataOfValueType();
        }

        @Override
        public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
            return new OneStepOfMultiSteps[0];
        }

        @Override
        public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
            throw new UnsupportedOperationException("测试替身，不参与保存");
        }

        @Override
        public List<UDFDesc> getLiteria() {
            return new ArrayList<>();
        }
    }
}

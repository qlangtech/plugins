package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.EnumFieldMode;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.TextAreaContentMode;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateTimeWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DecimalNumberWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DurationOfSecondWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.EnumWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.FileWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.MemorySizeOfMegaWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.PasswordWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.TextAreaWidget;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * 各 {@link UserInputWidgetType} 派生控件校验规则生成的端到端回归：
 * widget 生成 {@code validate<FieldId>} groovy 方法 → 随 {@code ActionFormPreview.Desc}
 * 编译 → {@code Descriptor#createValidateMap} 注册 → STRICT 校验生效。
 *
 * <p>覆盖三类生成物：边界类（Password/Decimal/Duration/MemorySize）、格式类
 * （Date/DateTime）、成员类（Enum），以及「无约束控件不生成方法」与 EnumWidget 的
 * 两个跳过分支（allowCustomizeInput / MULTIPLE）。IntNumberWidget 的链路细节由
 * {@code TestIntNumberWidgetValidateScript} 单独钉住。
 */
public class TestWidgetValidateRules {

    private static final String FIELD_NAME = "orderNum";

    /**
     * 组装单参数（指定控件）的参数池并现场生成运行时 Descriptor。
     */
    private Descriptor<?> buildDescriptor(UserInputWidgetType widget) {
        OntologyPluginMeta meta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, "temp_rules_domain")
                .setPluginIdVal("temp_rules_action");
        ActionParameterConfig param = new ActionParameterConfig();
        param.parameterId = FIELD_NAME;
        param.displayName = "订单数量";
        param.order = 0;
        PrimitiveDataType dataType = new PrimitiveDataType();
        dataType.widgetType = widget;
        dataType.required = true;
        param.dataType = dataType;

        ActionParameters params = new ActionParameters();
        params.parameters = new ArrayList<>();
        params.parameters.add(param);
        return ActionFormDescriptorBuilder.buildDescriptor(meta, params);
    }

    /**
     * 以 {@code _primaryVal = inputVal} 提交预览表单，走 STRICT 校验，返回校验结果。
     */
    private Descriptor.PluginValidateResult validate(String inputVal, Descriptor<?> formDesc
            , IControlMsgHandler msgHandler) {
        String body = """
                {"impl":"com.qlangtech.tis.plugin.ontology.impl.action.ActionFormPreview",
                 "vals":{"orderNum":{"_primaryVal":"%s"}}}
                """.formatted(inputVal);
        AttrValMap attrVals = AttrValMap.parseDescribableMap(Optional.empty(), JSONObject.parseObject(body)
                , formDesc, PropValRewrite.dftRewrite(), Function.identity());
        Assert.assertSame(formDesc, attrVals.descriptor);
        return attrVals.validateWithScope(msgHandler, new DefaultContext(), 0, 0, FormVaildateType.STRICT);
    }

    private static Method getValidateMethod(Descriptor<?> formDesc) throws NoSuchMethodException {
        return formDesc.getClass().getMethod("validate" + StringUtils.capitalize(FIELD_NAME)
                , IFieldErrorHandler.class, Context.class, String.class, String.class);
    }

    private static void assertNoValidateMethod(Descriptor<?> formDesc) {
        try {
            getValidateMethod(formDesc);
            Assert.fail("不应生成校验方法");
        } catch (NoSuchMethodException e) {
            // expected
        }
    }

    private static List<WidgetOptionConfig> options(String... vals) {
        List<WidgetOptionConfig> result = new ArrayList<>();
        for (String val : vals) {
            WidgetOptionConfig cfg = new WidgetOptionConfig();
            cfg.val = val;
            result.add(cfg);
        }
        return result;
    }

    private static EnumWidget buildEnumWidget(List<WidgetOptionConfig> options
            , EnumFieldMode showStyle, Boolean allowCustomizeInput) {
        EnumWidget widget = new EnumWidget();
        widget.options = options;
        widget.showStyle = showStyle;
        widget.allowCustomizeInput = allowCustomizeInput;
        return widget;
    }

    @Test
    public void testPasswordLengthBounds() {
        PasswordWidget widget = new PasswordWidget();
        widget.minLength = 6;
        widget.maxLength = 12;
        Descriptor<?> formDesc = buildDescriptor(widget);

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("长度小于 minLength 必须校验失败", validate("abc", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("长度不能小于6"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("区间内长度必须校验通过", validate("abcd1234", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("长度大于 maxLength 必须校验失败", validate("abcdefgh12345", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("长度不能大于12"));
    }

    @Test
    public void testPasswordNoBoundsNoValidateMethod() {
        assertNoValidateMethod(buildDescriptor(new PasswordWidget()));
    }

    @Test
    public void testEnumMembership() {
        Descriptor<?> formDesc = buildDescriptor(
                buildEnumWidget(options("a", "b", "c"), EnumFieldMode.DEFAULT, null));

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("选项外取值必须校验失败", validate("d", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("必须是候选项之一"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("选项内取值必须校验通过", validate("b", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testEnumAllowCustomizeInputSkipsMembership() {
        Descriptor<?> formDesc = buildDescriptor(
                buildEnumWidget(options("a", "b"), EnumFieldMode.DEFAULT, true));
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("允许自定义输入时选项外取值也应通过"
                , validate("custom-x", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testEnumMultipleModeNoValidateMethod() {
        // 多选模式下字段值是整条 JSON 数组文本（如 ["a","b"]），单值成员校验不适用 → 不生成
        assertNoValidateMethod(buildDescriptor(
                buildEnumWidget(options("a", "b"), EnumFieldMode.MULTIPLE, null)));
    }

    @Test
    public void testDecimalGuards() {
        DecimalNumberWidget widget = new DecimalNumberWidget();
        widget.min = 1.5;
        widget.max = 9.9;
        widget.scale = 2;
        Descriptor<?> formDesc = buildDescriptor(widget);

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("非数字应被格式守卫拦截（DECIMAL_NUMBER 无框架自带校验器）"
                , validate("abc", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("必须是数字"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("小数位超限必须校验失败", validate("1.123", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("小数位数不能超过2"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("越下界必须校验失败", validate("1.4", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("不能小于1.5"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("区间内取值必须校验通过", validate("2.55", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testDurationSecondRange() {
        DurationOfSecondWidget widget = new DurationOfSecondWidget();
        widget.min = Duration.ofSeconds(5);
        widget.max = Duration.ofSeconds(600);
        Descriptor<?> formDesc = buildDescriptor(widget);

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("越下界必须校验失败", validate("3", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("不能小于5秒"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("非数字应被格式守卫拦截", validate("10s", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("必须是整型数字"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("区间内取值必须校验通过", validate("60", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testMemoryMegaRange() {
        MemorySizeOfMegaWidget widget = new MemorySizeOfMegaWidget();
        widget.max = MemorySize.ofMebiBytes(512);
        Descriptor<?> formDesc = buildDescriptor(widget);

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("越上界必须校验失败", validate("1024", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("不能大于512MiB"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("区间内取值必须校验通过", validate("256", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testDateFormatGuard() {
        Descriptor<?> formDesc = buildDescriptor(new DateWidget());

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("非法格式必须校验失败", validate("2026/01/01", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("日期不符合格式yyyy-MM-dd"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("尾部夹带垃圾必须校验失败", validate("2026-01-01abc", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("日期不符合格式yyyy-MM-dd"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("合法日期必须校验通过", validate("2026-01-01", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testDateTimeFormatGuard() {
        // displayFormat 未配置 → 回落 TimeFormat.yyyyMMdd_HH_mm_ss（pattern: yyyy-MM-dd HH:mm:ss）
        Descriptor<?> formDesc = buildDescriptor(new DateTimeWidget());

        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertFalse("缺时间部分的串必须校验失败", validate("2026-01-01", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("日期时间不符合格式"));

        msgHandler = Mockito.mock(IControlMsgHandler.class);
        Assert.assertTrue("符合 pattern 的串必须校验通过"
                , validate("2026-01-01 10:30:00", formDesc, msgHandler).isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    @Test
    public void testConstraintFreeWidgetsNoValidateMethod() {
        assertNoValidateMethod(buildDescriptor(new InputTextWidget()));
        TextAreaWidget textArea = new TextAreaWidget();
        // appendExtraPropsJson 对 contentMode 有 requireNonNull，fixture 必须给个合法值
        textArea.contentMode = TextAreaContentMode.markdown;
        assertNoValidateMethod(buildDescriptor(textArea));
        assertNoValidateMethod(buildDescriptor(new FileWidget()));
    }
}

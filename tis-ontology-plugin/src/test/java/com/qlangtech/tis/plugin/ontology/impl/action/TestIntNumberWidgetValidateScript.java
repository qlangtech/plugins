package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.IntNumberWidget;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@link IntNumberWidget#generateParamVaildateScript} 校验脚本生成链路试验（也是回归）。
 *
 * <p>把这条链路端到端钉住：widget 生成 {@code validate<FieldId>} groovy 方法 →
 * 随 {@code ActionFormPreview.Desc} 一起编译 → {@code Descriptor#createValidateMap}
 * 构造期反射注册 → {@code AttrValMap#validateWithScope} 校验期按字段名查表调用。
 *
 * <p>注册契约（{@code createValidateMap}）：恰好 4 参且参数类型 {@code ==} 精确匹配
 * {@code (IFieldErrorHandler, Context, String, String)}、返回 primitive {@code boolean}，
 * 方法名 {@code validate + 首字母大写字段名}。fixture 构造镜像
 * {@code TestActionFormDescriptorBuilder#testExplicitDescriptorRoundTrip}。
 */
public class TestIntNumberWidgetValidateScript {

    private static final String FIELD_NAME = "orderNum";

    /**
     * 组装单参数（Int_Number 控件）的参数池并现场生成运行时 Descriptor。
     */
    private Descriptor<?> buildDescriptor(Integer min, Integer max) {
        OntologyPluginMeta meta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, "temp_validate_domain")
                .setPluginIdVal("temp_validate_action");
        ActionParameterConfig param = new ActionParameterConfig();
        param.parameterId = FIELD_NAME;
        param.displayName = "订单数量";
        param.order = 0;
        PrimitiveDataType dataType = new PrimitiveDataType();
        IntNumberWidget widget = new IntNumberWidget();
        widget.min = min;
        widget.max = max;
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

    /**
     * 生成的 Desc 上必须存在与 createValidateMap 注册契约完全一致的 4 参 boolean 方法。
     */
    @Test
    public void testValidateMethodRegisteredWithExactSignature() throws Exception {
        Descriptor<?> formDesc = buildDescriptor(3, 10);
        Method validateMethod = getValidateMethod(formDesc);
        Assert.assertEquals("校验方法必须返回 primitive boolean", boolean.class, validateMethod.getReturnType());
    }

    @Test
    public void testValueBelowMinInvalid() {
        Descriptor<?> formDesc = buildDescriptor(3, 10);
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Descriptor.PluginValidateResult result = validate("1", formDesc, msgHandler);
        Assert.assertFalse("值小于 min 必须校验失败", result.isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("不能小于3"));
    }

    @Test
    public void testValueAboveMaxInvalid() {
        Descriptor<?> formDesc = buildDescriptor(3, 10);
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Descriptor.PluginValidateResult result = validate("99", formDesc, msgHandler);
        Assert.assertFalse("值大于 max 必须校验失败", result.isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("不能大于10"));
    }

    @Test
    public void testValueInRangeValid() {
        Descriptor<?> formDesc = buildDescriptor(3, 10);
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Descriptor.PluginValidateResult result = validate("5", formDesc, msgHandler);
        Assert.assertTrue("区间内取值必须校验通过", result.isValid());
        Mockito.verify(msgHandler, Mockito.never())
                .addFieldError(Mockito.any(Context.class), Mockito.any(String.class), Mockito.any(String.class));
    }

    /**
     * INT_NUMBER 字段类型自带追加的 {@code Validator.integer}
     * （FormFieldType.INT_NUMBER 构造参数 getAppendValueValidator），非法格式在声明
     * validators 阶段就被框架拦截，轮不到生成脚本里的 isLong 守卫 —— 守卫只是
     * validateXxx 被直接 invoke 时的第二道保险。
     */
    @Test
    public void testNonNumericValueRejectedByFrameworkIntegerValidator() {
        Descriptor<?> formDesc = buildDescriptor(3, 10);
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Descriptor.PluginValidateResult result = validate("abc", formDesc, msgHandler);
        Assert.assertFalse("非数字应被框架自带的 Validator.integer 拦截", result.isValid());
        Mockito.verify(msgHandler).addFieldError(Mockito.any(Context.class), Mockito.eq(FIELD_NAME)
                , Mockito.contains("必须是整型数字"));
    }

    /**
     * 只配 min（或只配 max）时只生成对应的越界块。
     */
    @Test
    public void testMinOnlyGeneratesOnlyMinBlock() {
        ValidateScriptBuffer buf = new ValidateScriptBuffer();
        ActionParameterConfig param = new ActionParameterConfig();
        param.parameterId = FIELD_NAME;
        IntNumberWidget widget = new IntNumberWidget();
        widget.min = 2;
        widget.generateParamVaildateScript(buf, param);
        String script = buf.getContent().toString();
        Assert.assertTrue("应包含方法签名", script.contains(
                "public boolean validate" + StringUtils.capitalize(FIELD_NAME)
                        + "(IFieldErrorHandler msgHandler, Context context, String fieldName, String value)"));
        Assert.assertTrue(script.contains("if (v < 2)"));
        Assert.assertFalse("未配置 max 不应生成上界块", script.contains("v >"));
    }

    /**
     * min/max 都没配置时不生成校验方法，Desc 上也不应有 validateXxx 残留。
     */
    @Test
    public void testNoRangeNoValidateMethodGenerated() {
        Descriptor<?> formDesc = buildDescriptor(null, null);
        try {
            getValidateMethod(formDesc);
            Assert.fail("min/max 都未配置不应生成校验方法");
        } catch (NoSuchMethodException e) {
            // expected
        }
    }
}

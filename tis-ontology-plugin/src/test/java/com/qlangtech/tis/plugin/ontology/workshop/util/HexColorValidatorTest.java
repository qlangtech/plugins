package com.qlangtech.tis.plugin.ontology.workshop.util;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * {@link HexColorValidator} 的格式判定与错误上报。
 *
 * <p>本类是 {@code SectionStyleConfig.backgroundColor} 与
 * {@code CustomButtonColor.customColor} 共用的校验规则，任何一侧放宽/收紧都会影响
 * 两处表单，因此边界值在这里钉死。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class HexColorValidatorTest {

    @Test
    public void testAcceptCssHexColors() {
        String[] valid = {
                "#fff", "#FFF",       // 三位
                "#abcd", "#ABCD",     // 四位（带 alpha）
                "#f5222d", "#AABBCC", // 六位
                "#f5222dcc", "#00FF00FF" // 八位（带 alpha）
        };
        for (String value : valid) {
            Assert.assertTrue("应接受合法色值: " + value, HexColorValidator.isHexColor(value));
        }
    }

    @Test
    public void testRejectInvalidColors() {
        String[] invalid = {
                null, "",
                "#", "#ff", "#fffff", "#fffffff", // 位数不对
                "fff", "f5222d",                   // 缺 #
                "#ggg", "#12345g",                 // 非十六进制字符
                "red", "rgb(255,0,0)",             // 非十六进制写法
                " #fff", "#fff ",                  // 带空白不 trim
                "##fff"
        };
        for (String value : invalid) {
            Assert.assertFalse("应拒绝非法色值: " + value, HexColorValidator.isHexColor(value));
        }
    }

    @Test
    public void testValidateReportsErrorForInvalidValue() {
        IFieldErrorHandler msgHandler = Mockito.mock(IFieldErrorHandler.class);
        Context context = Mockito.mock(Context.class);

        Assert.assertFalse(HexColorValidator.validate(msgHandler, context, "backgroundColor", "red"));
        Mockito.verify(msgHandler).addFieldError(
                Mockito.same(context), Mockito.eq("backgroundColor"), Mockito.contains("十六进制"));
        Mockito.verifyNoMoreInteractions(msgHandler);
    }

    @Test
    public void testValidatePassesSilentlyForValidValue() {
        IFieldErrorHandler msgHandler = Mockito.mock(IFieldErrorHandler.class);
        Context context = Mockito.mock(Context.class);

        Assert.assertTrue(HexColorValidator.validate(msgHandler, context, "backgroundColor", "#f5222d"));
        Mockito.verifyNoInteractions(msgHandler);
    }
}

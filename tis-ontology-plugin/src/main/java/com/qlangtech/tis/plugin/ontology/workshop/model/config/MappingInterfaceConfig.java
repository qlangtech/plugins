package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.ArrayList;
import java.util.List;

/**
 * 「开启」形态的变量接口配置：把外部接口的输入参数映射到本模块内的变量。
 *
 * <p>开关由子类类型承担，故本类没有任何 {@code enabled} 字段 —— 能构造出本类的实例，
 * 就意味着接口映射是开启的。对照 {@link NoneInterfaceConfig}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class MappingInterfaceConfig extends VariableInterfaceConfig {

    public static final String KEY_INTERFACE_ID = "interfaceId";
    public static final String KEY_INPUTS = "inputs";

    /**
     * 对外暴露的接口名（Palantir 的 module interface 概念）。
     *
     * <p>声明为 {@code INPUTTEXT} 而非 {@code SELECTABLE} 是<b>有意的取舍</b>：
     * 本仓库目前不存在「模块接口注册表」，没有可供给的候选源，用 SELECTABLE 只会渲染出
     * 一个永远为空的必选下拉。语义上它就是一个自由命名。若将来有了注册表，
     * 再升级为 SELECTABLE + {@code registerSelectOptions}。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String interfaceId;

    /**
     * 接口输入映射表，每行是「外部参数名 → 模块内变量名」。
     *
     * <p>非必填：空列表表示该接口没有输入参数，是合法形态
     * （{@link InterfaceInputCreatorFactory} 对 null 行数组同样按空列表处理）。
     */
    @FormField(ordinal = 1, type = FormFieldType.MULTI_SELECTABLE)
    public List<InterfaceInputRow> inputs = new ArrayList<>();

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Module Interface";
        }

        @Override
        public String shortComment() {
            return "对接外部接口";
        }
    }
}

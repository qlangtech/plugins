package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.IMultiElement;

import java.io.Serializable;

/**
 * {@link PageRoutingConfig} 中<b>单条路由参数映射</b> —— 「URL 参数名 → 本模块内的变量」。
 *
 * <p>形状与 {@link InterfaceInputRow} 同构（都是「外部键名 → 模块内变量」），但<b>刻意不合并</b>：
 * 两者的键处于两个不同的命名空间（外部接口的形参 vs URL query 参数），列头与文案也不同，
 * 且合并会让「给其中一个加字段」变成对另一个的隐式变更。
 *
 * <p>{@code variableName} 存的是变量名而非 UUID，理由见 {@link InterfaceInputRow} 的类注释。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see RoutingParamCreatorFactory
 */
public class RoutingParamRow implements Describable<RoutingParamRow>, IMultiElement, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * URL 上的参数名（query string 的键）。
     *
     * <p>{@code identity = true}：同一个参数名在映射表里出现两次，后一行会静默覆盖前一行。
     * {@link RoutingParamCreatorFactory#parsePostMCols} 据此把重复判为错误。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String urlParam;

    /**
     * 该参数回填到哪个变量（当前 Workshop Module 内），候选项由
     * {@link RoutingParamCreatorFactory#appendExternalJsonProp} 下发。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String variableName;

    /**
     * 作为元组行的标识，取 {@link #urlParam}。
     */
    @Override
    public String getName() {
        return this.urlParam;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<RoutingParamRow> {
    }
}

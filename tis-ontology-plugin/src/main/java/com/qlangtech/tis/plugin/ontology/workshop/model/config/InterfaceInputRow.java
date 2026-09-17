package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.IMultiElement;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;

import java.io.Serializable;

/**
 * {@link MappingInterfaceConfig} 中<b>单条接口输入映射</b> —— 「外部接口的参数名 → 本模块内的变量」。
 *
 * <h3>为什么是一个独立类型</h3>
 * 与 {@code workshop.widget.FilterItemConfig} 同一个理由：这是一对「键 → 变量」的结构化数据，
 * 让构建者手写 JSON 数组字符串既没有表单校验，也没有变量下拉可选项。
 * 声明成 {@code Describable} 后由 TIS 的元组行编辑器（{@link InterfaceInputCreatorFactory}）渲染。
 *
 * <h3>为什么第二个字段叫 variableName 而不是 variableId</h3>
 * 选项来源是「当前模块的变量列表」，而 {@link WorkshopVariable} 上有两个标识：
 * {@code id}（自动生成的 UUID，决定落盘文件名）与 {@code name}（模块内唯一引用名）。
 * 用户能在下拉里选、也是其他 Widget 绑定字段实际存的那个值，是 <b>name</b>
 * —— {@code WidgetOptionHelper} 的各个 {@code getXxxVariableOptions()} 返回的都是
 * {@code IdentityName.create(v.getName())}，{@code objectSetVar} / {@code dataSourceVar}
 * 等字段存的也都是 name。叫 {@code variableId} 会与「存的是 UUID」这一读法冲突。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 * @see InterfaceInputCreatorFactory
 */
public class InterfaceInputRow implements Describable<InterfaceInputRow>, IMultiElement, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 外部接口的输入参数名。
     *
     * <p>{@code identity = true}：同一个参数在映射表里出现两次没有意义 ——
     * 后一行会静默覆盖前一行，而构建者看不出是哪一行生效。
     * {@link InterfaceInputCreatorFactory#parsePostMCols} 据此把重复判为错误。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String parameter;

    /**
     * 该参数在平台侧映射到的变量名（当前 Workshop Module 内）。
     *
     * <p>取值来自 {@link InterfaceInputCreatorFactory} 随线格式下发的变量候选列表
     * （见其 {@code appendExternalJsonProp}），行编辑器渲染成下拉框。
     * 这里声明为普通 {@code INPUTTEXT} 而非 {@code SELECTABLE}：候选集是<b>逐行相同的</b>，
     * 而元组行的字段拿不到所在行的上下文，选项改由工厂统一挂在 {@code _eprops.enum} 上
     * —— 与 {@code FilterItemConfig.property} 的做法一致。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String variableName;

    /**
     * 作为元组行的标识：{@link IMultiElement#getName()} 被 {@code FormFieldType.SelectedItem}
     * 用作选项的 name 与 value，取 {@link #parameter} —— 它才是这一行的身份。
     */
    @Override
    public String getName() {
        return this.parameter;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<InterfaceInputRow> {
    }
}

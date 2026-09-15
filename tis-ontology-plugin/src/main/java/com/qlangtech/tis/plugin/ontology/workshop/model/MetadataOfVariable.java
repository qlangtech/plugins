package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;

import java.util.Optional;
import java.util.UUID;

/**
 * Workshop Variable 多步配置的<b>第一步</b>：变量的元数据（标识、名称、值类型）。
 *
 * <p>这三项之所以单独成步，是因为 {@link #type} 是后续所有定义方式选择的<b>前置条件</b>：
 * 只有一部分定义方式能产出某一类值（「对象集合定义」只能产出 {@code OBJECT_SET}，
 * 「函数计算」能产出绝大多数类型），映射表见
 * {@link com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig#TYPE_DEFINITIONS}。
 * 拆成两步后，用户在第二步看到的下拉只列出与第一步所选类型兼容的定义方式
 * （过滤逻辑见 {@link DefinitionOfVariable#descFilter}）。
 *
 * @see DefinitionOfVariable
 * @see WorkshopVariable
 */
public class MetadataOfVariable extends OneStepOfMultiSteps {

    /**
     * 唯一标识，决定变量的落盘文件名
     * （{@code ontology/{domain}/workshop_variables/{moduleName}/{id}.xml}）。
     *
     * <p><b>这里不能标 {@code identity = true}</b>：{@code OneStepOfMultiSteps} 不是
     * {@link com.qlangtech.tis.plugin.IdentityName}，框架的
     * {@code Descriptor.getPropertyTypes()} 对非 IdentityName 的类遇到 identity 字段会直接抛异常。
     * 真正的 identity 由宿主 {@link WorkshopVariable} 的占位字段承担，
     * 其 {@code identityValue()} 会回到本类的 {@link #id}。
     *
     * <p>与改造前 {@code WorkshopVariable.id} 的语义完全一致：构造时生成 UUID，
     * 创建后由前端回传、或由后端从 URL 参数覆盖（见
     * {@code WorkshopVariable.DefaultDescriptor#doUpdate}）。
     */
    @FormField(ordinal = -1, type = FormFieldType.INPUTTEXT)
    public String id;

    /**
     * 模块内唯一引用名（大小写不敏感），widget 通过它反向引用变量。
     * 唯一性由 {@code WorkshopModuleService} 在落盘前校验。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    /**
     * 取值集合固定，用 ENUM 让选项由 {@link VariableType} 直接下发
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public VariableType type;

    public MetadataOfVariable() {
        this.id = UUID.randomUUID().toString();
    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc implements DescriptorUseableShortComment {

        @Override
        public String getStepDescription() {
            return "Metadata";
        }

        @Override
        public Step getStep() {
            return Step.Step1;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new DefinitionOfVariable.Desc());
        }

        @Override
        public boolean isFinalStep() {
            return false;
        }

        @Override
        public String shortComment() {
            return "设置变量的标识、名称与值类型";
        }
    }
}

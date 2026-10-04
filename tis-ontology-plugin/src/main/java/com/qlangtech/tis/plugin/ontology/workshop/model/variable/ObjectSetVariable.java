package com.qlangtech.tis.plugin.ontology.workshop.model.variable;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.FunctionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link VariableType#OBJECT_SET} —— 一组 Ontology 对象，含其属性与关系。
 *
 * <p>本类<b>没有自己的字段</b>：变量类型由 Java 类型承担，其余配置全部继承自
 * {@link WorkshopVariable}。可用哪些定义方式见
 * {@code VariableDefinitionConfig#TYPE_DEFINITIONS} 中本类型对应的那一行
 * —— 不在这里重复，避免矩阵出现第二份会过期的副本。
 *
 * @see ObjectSetDefinitionConfig
 */
public class ObjectSetVariable extends WorkshopVariable {

    public List<OntologyProperty> getColsMeta() {

        if (this.definitionConfig instanceof ObjectSetDefinitionConfig objSetCfg) {
            return objSetCfg.getColsMeta();
        } else if (this.definitionConfig instanceof FunctionConfig funcCfg) {
            // FunctionConfig 确实在 VariableDefinitionConfig.TYPE_DEFINITIONS 的 OBJECT_SET 行里（保持不动），
            // 但函数输出的 schema 目前没有被建模：FunctionConfig 只有自由文本 functionId 与 inputs，
            // FunctionImplementation.returnType 是标量白名单，没有任何本体对象类型的表达，
            // functionId 也未与 OntologyFunction 建立引用。因此这里取不到 List<OntologyProperty>，
            // 只能给出可执行的报错，引导用户改用对象集合定义，而不是返回一个静默的空列列表。
            throw new IllegalStateException("对象集合变量 '" + this.getName() + "' 由函数计算定义（functionId="
                    + funcCfg.functionId + "），函数输出的 schema 未建模，无法推导列定义。"
                    + "如需按对象属性自动填充列，请把该变量的定义方式改为 'ObjectSet Definition（对象集合定义）'。");
        } else {
            throw new IllegalStateException("illegal type:" + this.definitionConfig.getClass().getName()
                    + " support:" + VariableDefinitionConfig.definitionsOf(this.getVariableType()).stream().map(Class::getName).collect(Collectors.joining(",")));
        }
    }

    @Override
    public VariableType getVariableType() {
        return VariableType.OBJECT_SET;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {

        public DefaultDescriptor() {
            super(VariableType.OBJECT_SET);
        }
    }
}

package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.qlangtech.tis.aiagent.llm.ITISJsonSchema;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary;
import com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker;
import com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType;
import com.qlangtech.tis.util.DescriptorsJSONForAIPrompt;
import com.qlangtech.tis.util.DescriptorsMeta;

import java.util.Collections;
import java.util.function.Supplier;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/8
 */
@SuppressWarnings("all")
public class OntologyResourceInferenceConfig {
    private final Ontology.OntologyEnum ontologyEnum;
    private final Supplier<ITISJsonSchema> jsonSchema;
    private final String description;
    private final String llmPrompt;

    public OntologyResourceInferenceConfig(Ontology.OntologyEnum ontologyEnum //
            , Supplier<ITISJsonSchema> jsonSchema, String description, String llmPrompt) {
        this.ontologyEnum = ontologyEnum;
        this.jsonSchema = jsonSchema;
        this.description = description;
        this.llmPrompt = llmPrompt;
    }

    public static OntologyResourceInferenceConfig sharedPropertyConfig = buildSharedPropertyItemSchema();
    public static OntologyResourceInferenceConfig linkerType = buildLinkTypeItemSchema();
    public static OntologyResourceInferenceConfig valueType = buildValueTypeItemSchema();
    public static OntologyResourceInferenceConfig glossary = buildGlossaryItemSchema();

    public String getPrompt() {
        return llmPrompt;
    }

    public String getDescription() {
        return description;
    }

    public String getInferenceType(){
        return ontologyEnum.getTypeIdentity();
    }

    public ITISJsonSchema getJsonSchema() {
        return this.jsonSchema.get();
    }

    /**
     *
     * @return
     * @see OntologySharedProperty
     */
    private static OntologyResourceInferenceConfig buildSharedPropertyItemSchema() {


        return new OntologyResourceInferenceConfig(Ontology.OntologyEnum.SharedProperty, () -> {
            DescriptorsJSONForAIPrompt descriptorsJSON =
                    new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologySharedProperty.DefaultDesc()),
                            true
                            , (b, desc) -> {
                        InferenceParse.addTargetColumns2Schema(b);
                        InferenceParse.add2SchemaBuilder(b);
                    }, (attr, addedProp) -> false
                    );
            DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();
            return meta.getPluginJsonSchema().values().iterator().next();
        }
                , "Shared Property（sharedProperties，共享属性）"//
                , """
                多个表中出现的相同语义的属性，适合抽取为共享属性复用。
                判断依据：多张表中出现相同名称且相同类型的列（如 create_time, update_time, status, currency_code 等）。
                至少在2张表中出现才考虑抽取。
                """);
    }

    /**
     *
     * @return
     * @see OntologyLinker
     */
    private static OntologyResourceInferenceConfig buildLinkTypeItemSchema() {


        return new OntologyResourceInferenceConfig(Ontology.OntologyEnum.Linker, () -> {
            DescriptorsJSONForAIPrompt descriptorsJSON =
                    new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologyLinker.DefaultDesc()),
                            true, (b, desc) -> {
                        // 推断元数据
                        InferenceParse.add2SchemaBuilder(b);
                    }, (attr, addedProp) -> false);

            DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();
            ITISJsonSchema schema = meta.getFirstPluginJsonSchema();
            return schema;
        }  //
                , "Link Type（linkTypes）（关联关系）" //
                , """
                表之间的关联关系，有2种类型：
                - Object type foreign keys (token=1): 通过外键关联，用于一对一或一对多关系。
                  判断依据：某表的列名为 xxx_id 且另一张表名为 xxx 且有 id 主键。
                - Backing object type (token=3): 通过中间对象类型关联，用于带属性的多对多关系。
                  判断依据：某表有两个外键列但还有其他业务属性列。
                
                ### ⚠️ 核心优先级与互斥规则（最高优先级）
                
                请严格遵守以下流程，确保输出结果的准确性和通用性。
                
                #### 🔍 1. 列名识别基础规则（通用过滤）
                在进行任何关系推断前，先应用此规则过滤列名：
                - **ID 特征要求**：只有列名包含明确 ID 特征（如 `_id`, `ID`, `_key`）时，才可被识别为外键或主键。
                - **跳过非典型名**：如果列名仅为 `code`, `country_code`, `number`, `no` 等非典型命名，即使逻辑上像外键，也**必须跳过**，不生成任何 Link Type。
                - **主键优先**：优先识别 `pk=true` 的列为关键连接点。
                
                #### 🚀 2. 两阶段物理隔离流程
                
                请按顺序执行，且前一阶段的结果会物理影响后一阶段的输入。
                
                ##### 阶段 A：中间表（Backing Object）识别与吞噬 (Token 3)
                1. **扫描目标**：查找同时满足以下条件的表：
                   - 包含**至少两个**符合上述“ID 特征”的外键列（例如：`store_id` + `product_id`）。
                   - 包含除外键外的**业务属性列**（如 `amount`, `date`, `stock`, `units`）。
                2. **操作**：
                   - 为该表生成 Token 3 (`RelationshipTypeBackingObjectType`)。
                   - **【物理移除】**：立即将该表从你的“可用表池”中移除。在后续所有步骤中，你**看不见**这张表。
                
                例如，有以下三张表：
                * store(store_id(pk=true),store_name)
                * product(product_id(pk=true),product_name)
                * sales(sale_id(pk=true),store_id,product_id,create_date)
                
                将 sales 作为`Token 3` 中间表 以store_id连接store(store_id)，以product_id连接product(product_id)，注意：无论sales(sale_id) 是否是`pk`都不影响`Token 3`的识别
                   
                
                ##### 阶段 B：简单外键连接 (Token 1)
                1. **扫描目标**：仅在**未被移除**的表中，寻找“父表（主键）”与“子表（外键）”的一对多关系。
                2. **操作**：
                   - 生成 Token 1 (`RelationshipTypeObjectTypeForeignKeys`)。
                3. **【关键限制】**：因为你已经在阶段 A 移除了中间表，所以这里**绝对不可能**出现中间表作为“子表”的连接。如果出现了，说明阶段 A 失败。
                
                #### ✅ 3. 通用最终验证
                在输出 JSON 前，执行以下自检：
                1. **互斥检查**：检查所有 Token 3 的 `joinObjectType.objectType`（中间表名）。
                   - 确保这些表名**不出现在**任何 Token 1 的 `right.objectType`（子表侧）中。
                2. **数量逻辑**：
                   - 如果输入包含 N 张中间表，则最终 Link Types 数量应为 **N**（仅 Token 3），或者 **N + 剩余维度表连接数**。
                   - 绝对不允许出现“中间表既在 Token 3 又在 Token 1”的情况。                
                """);
    }


    /**
     * 利用 {@link DescriptorsJSONForAIPrompt} 自动生成 {@link OntologyValueType} 的 schema。
     * 由于 {@link OntologyValueType} 实现 {@link com.qlangtech.tis.extension.MultiStepsSupportHost}，
     * 自动生成的 schema 形如 <code>{ impl, vals:{ multiStepsSavedItems:[{impl,vals},{impl,vals}] } }</code>，
     * 与 {@link com.qlangtech.tis.extension.OneStepOfMultiSteps#parseStepsPlugin} 期望的反序列化格式天然对齐。
     * 外层再平铺 sourceColumn / confidence / reason 三个推断元数据字段。
     *
     * @return
     * @see OntologyValueType
     */
    private static OntologyResourceInferenceConfig buildValueTypeItemSchema() {


        return new OntologyResourceInferenceConfig(Ontology.OntologyEnum.ValueType, () -> {
            DescriptorsJSONForAIPrompt descriptorsJSON =
                    new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologyValueType.DefaultDesc()), true
                            , (b, desc) -> {
                        // 推断元数据
                        // b.addProperty("sourceColumn", TISJsonSchema.FieldType.String, "来源列（表名.列名）");
                        InferenceParse.addTargetColumns2Schema(b);

                        InferenceParse.add2SchemaBuilder(b);

                    }, (attr, addedProp) -> false);

            DescriptorsMeta meta
                    = descriptorsJSON.getDescriptorsJSON();

            // host schema 形如 { impl, vals:{ multiStepsSavedItems:[...] } }
            ITISJsonSchema hostSchema = meta.getFirstPluginJsonSchema();

            return hostSchema;
        }  //
                , "Value Types（valueTypes，值类型 + 约束）" //
                , """
                列值有明确约束的属性，适合定义为值类型。
                判断依据：
                - 列注释中包含枚举值列表（如 "PENDING/PAID/SHIPPED"）→ Enum 约束
                - 列类型暗示范围约束（如 VARCHAR(3) 可能是国家代码）→ Range 约束                
                """);
    }

    /**
     * @see OntologyGlossary
     */
    private static OntologyResourceInferenceConfig buildGlossaryItemSchema() {


        return new OntologyResourceInferenceConfig(Ontology.OntologyEnum.Glossary, () -> {
            DescriptorsJSONForAIPrompt descriptorsJSON =
                    new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologyGlossary.DefaultDesc()), true,
                            (builder, desc) -> {
                                InferenceParse.add2SchemaBuilder(builder);
                            }, (attr, addedProp) -> false);

            DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();

            ITISJsonSchema schema = meta.getFirstPluginJsonSchema();
            StringBuilder prompt = new StringBuilder();
            schema.appendFieldDescToPrompt(prompt);
            return schema;
        }  //
                , "Glossary（glossaries，业务术语 / 同义词词典）"//
                , """
                业务术语字典，用于 ChatBI 自然语言到 SQL 的桥接，把用户口语化的业务名词映射到本体对象。
                有三种 target 类型：
                - GlossaryTargetOT: 业务实体名 → 某个 ObjectType。
                  判断依据：表名对应业务实体（如 customer/orders/products）。
                  示例：term="Customer"，synonyms=["用户","User","buyer","购买方"]，target.targetType="GlossaryTargetOT"，target.objectType="customer"
                - GlossaryTargetProperty: 业务字段名 → 某个 ObjectType 的某列。
                  判断依据：业务上有明确语义的列（如 amount/status/created_at）。
                  示例：term="OrderAmount"，synonyms=["金额","总额","订单总额"]，target.targetType="GlossaryTargetProperty"，target.objectType="orders"，target.propertyName="amount"
                - GlossaryTargetMetricExpr: 业务指标 → 自定义 SQL 表达式。
                  判断依据：常见业务指标（如总销售额、活跃用户数、客单价）可由聚合 SQL 表达。
                  示例：term="SalseGMV"，synonyms=["销售总额","GMV"]，target.targetType="GlossaryTargetMetricExpr"，target.sql="SUM(orders.amount)"
                
                同义词请尽量覆盖：中文同义词、英文同义词、口语化表达、行业术语。
                优先从列注释/表名中提取业务名词，避免编造。
                `注意`：以上term必须符合正则式"[A-Z\\\\da-z_]+"规范                     
                """);
    }
}

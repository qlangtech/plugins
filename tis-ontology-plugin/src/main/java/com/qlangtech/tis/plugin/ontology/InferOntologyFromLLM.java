/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.ontology;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.aiagent.core.IAgentContext;
import com.qlangtech.tis.aiagent.llm.FlatJsonToTisConverter;
import com.qlangtech.tis.aiagent.llm.ITISJsonSchema;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.aiagent.llm.TISJsonSchema;
import com.qlangtech.tis.aiagent.llm.UserPrompt;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.impl.DefaultGroovyShellFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ds.manipulate.ManipuldateUtils;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary;
import com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker;
import com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.DescriptorsJSONForAIPrompt;
import com.qlangtech.tis.util.DescriptorsMeta;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.qlangtech.tis.manage.common.UserProfile.KEY_FIELD_LLM_NAME;
import static com.qlangtech.tis.plugin.ontology.OntologyDomain.NAME_ONTOLOGY_DOMAIN;
import static com.qlangtech.tis.plugin.ontology.OntologyDomain.ONTOLOGY_DOMAIN;

/**
 * 利用 LLM 从已有 ObjectType 的表结构中推断 Link Type、Shared Property、Value Type
 * <p>
 * 用户在本体域管理界面触发此操作后，系统收集当前 domain 下所有 ObjectType 的 schema，
 * 组装 prompt 提交给 LLM，LLM 返回结构化 JSON 建议列表，前端展示供用户确认。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/20
 */
@SuppressWarnings("all")
public class InferOntologyFromLLM extends OntologyDomainManipulate {

    //    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require, Validator.identity})
    //    public String ontologyDomain;
    private static final String KEY_LINK_TYPES = "linkTypes";
    private static final String KEY_SHARED_PROPERTIES = "sharedProperties";
    private static final String KEY_VALUE_TYPES = "valueTypes";
    private static final String KEY_GLOSSARIES = "glossaries";
    /**
     * 大模型接口
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, validate = {Validator.identity})
    public String llm;

    /**
     * 需要导入到本体域的表对象
     */
    @FormField(ordinal = 200, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<IdentityName> targetTables = Lists.newArrayList();

    public LLMProvider getLlmProvider() {
        return LLMProvider.load(Objects.requireNonNull(IPluginContext.getThreadLocalInstance()), llm);
    }

    private static OntologyPluginMeta getOntologyPluginMeta(IPluginContext pluginContext, Optional<Context> context) {
        final Context ctx = context.orElseThrow();
        ManipulateItemsProcessor itemsProcessor = ManipuldateUtils.instance(pluginContext, ctx,
                null, (meta) -> {
                    UploadPluginMeta.putPluginMeta(ctx, meta);
                });

        OntologyPluginMeta ometa = OntologyPluginMeta.createPluginMeta(itemsProcessor.getPluginMeta());

        if (StringUtils.isEmpty(ometa.getDomain())) {
            throw new IllegalArgumentException("property ontologyDomain can not be null");
        }
        return ometa;
    }

    @Override
    public void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta pluginMeta,
                                   Optional<Context> context) {
        final Context ctx = context.orElseThrow();
        //        ManipulateItemsProcessor itemsProcessor = ManipuldateUtils.instance(pluginContext, ctx,
        //                null, (meta) -> {
        //                    UploadPluginMeta.putPluginMeta(ctx, meta);
        //                });

        OntologyPluginMeta ometa = getOntologyPluginMeta(pluginContext, context);

        List<OntologyObjectType> objectTypes = OntologyObjectType.loadAll(ometa.getDomain());
        if (objectTypes.isEmpty()) {
            throw new IllegalStateException("domain '" + ometa.getDomain()
                    + "' has no ObjectType, please export tables first");
        }

        JSONObject tablesPayload = buildTablesPayload(objectTypes);
        String systemPrompt = this.buildSystemPrompt();
        String userPrompt = tablesPayload.toJSONString();

        // UserProfile userProfile = UserProfile.load(pluginContext, true);
        LLMProvider llmProvider = this.getLlmProvider();

        /**
         * 大模型推断
         */
        LLMProvider.LLMResponse response = llmProvider.chatJson(
                IAgentContext.createNull(),
                new UserPrompt("Infer ontology relations", userPrompt),
                Collections.singletonList(systemPrompt),
                buildOutputJsonSchema());

        if (!response.isSuccess() || response.getJsonContent() == null) {
            throw new IllegalStateException("LLM inference failed: "
                    + (response.getErrorMessage() != null ? response.getErrorMessage() : "no response"));
        }
        JSONObject jsonContent = response.getJsonContent();

        DeserializeOntologyRes ontologyRes = deserializeOntologyRes(ometa.getDomain(), pluginContext, jsonContent, ctx);

        ontologyRes.create(pluginContext);

        //  System.out.println(linkTypes);

        // 把 vals 内的 primitive 字段（如 MetadataOfValueType.type:int）包装成 {_primaryVal: val}，
        // 使其能被 AttrVals#parseAttrValMap 反序列化。详见 normalizeValsForReparse 注释。
        // normalizeValsForReparse(jsonContent);

        //  pluginContext.setBizResult(context.orElseThrow(), jsonContent);
    }

    public DeserializeOntologyRes deserializeOntologyRes(String ontologyDomain, IPluginContext pluginContext,
                                                         JSONObject jsonContent
            , Context ctx) {

        JSONArray linkTypesJsonArray = jsonContent.getJSONArray(KEY_LINK_TYPES);
        JSONArray sharedPropsJsonArray = jsonContent.getJSONArray(KEY_SHARED_PROPERTIES);
        JSONArray valueTypesJsonArray = jsonContent.getJSONArray(KEY_VALUE_TYPES);
        JSONArray glossariesJsonArray = jsonContent.getJSONArray(KEY_GLOSSARIES);

        List<Pair<OntologyLinker, InferenceParse>> linkTypes //
                = createOntologyResources(pluginContext, linkTypesJsonArray, ctx);
        List<Pair<OntologySharedProperty, InferenceParse>> sharedProps  //
                = createOntologyResources(pluginContext, sharedPropsJsonArray, ctx);
        List<Pair<OntologyValueType, InferenceParse>> valueTypes //
                = createOntologyResources(pluginContext, valueTypesJsonArray, ctx);
        List<Pair<OntologyGlossary, InferenceParse>> glossaries //
                = createOntologyResources(pluginContext, glossariesJsonArray, ctx);

        return new DeserializeOntologyRes(ontologyDomain, linkTypes, sharedProps, valueTypes, glossaries);

    }

    private static class DeserializeOntologyRes {
        private final List<Pair<OntologyLinker, InferenceParse>> linkTypes;
        private final List<Pair<OntologySharedProperty, InferenceParse>> sharedProps;
        private final List<Pair<OntologyValueType, InferenceParse>> valueTypes;
        private final List<Pair<OntologyGlossary, InferenceParse>> glossaries;

        private final String ontologyDomain;


        private final ConcurrentMap<String, OntologyObjectType> updatedObjectType = Maps.newConcurrentMap();


        public DeserializeOntologyRes(String ontologyDomain, List<Pair<OntologyLinker, InferenceParse>> linkTypes,
                                      List<Pair<OntologySharedProperty, InferenceParse>> sharedProps //
                , List<Pair<OntologyValueType, InferenceParse>> valueTypes, List<Pair<OntologyGlossary,
                        InferenceParse>> glossaries) {
            this.linkTypes = linkTypes;
            this.sharedProps = sharedProps;
            this.valueTypes = valueTypes;
            this.glossaries = glossaries;
            this.ontologyDomain = ontologyDomain;
        }

        private OntologyObjectType getObjectType(final TargetProperty targetProperty) {
            if (targetProperty == null) {
                throw new IllegalArgumentException("param objTypeName can not be empty");
            }
            return updatedObjectType.computeIfAbsent(targetProperty.objectType(), (key) -> {
                return Ontology.loadObjectTypeDetail(ontologyDomain, targetProperty.objectType());
            });
        }

        public void create(IPluginContext pluginContext) {
            InferenceParse inferenceParse = null;

            OntologyObjectType objectType = null;
            for (Pair<OntologyLinker, InferenceParse> linker : linkTypes) {
                Ontology.OntologyEnum.Linker.save(pluginContext, ontologyDomain, linker.getKey());
            }
            for (Pair<OntologySharedProperty, InferenceParse> sharedProperty : sharedProps) {
                Ontology.OntologyEnum.SharedProperty.save(pluginContext, ontologyDomain, sharedProperty.getKey());
                inferenceParse = sharedProperty.getValue();
                for (TargetProperty targetProperty : inferenceParse.getTargetProps()) {
                    objectType = this.getObjectType(targetProperty);
                    objectType.setSharedProperty(targetProperty, sharedProperty.getKey());
                }
            }
            for (Pair<OntologyValueType, InferenceParse> valueType : valueTypes) {
                Ontology.OntologyEnum.ValueType.save(pluginContext, ontologyDomain, valueType.getKey());
                inferenceParse = valueType.getValue();
                for (TargetProperty targetProperty : inferenceParse.getTargetProps()) {
                    objectType = this.getObjectType(targetProperty);
                    objectType.setValeType(targetProperty, valueType.getKey());
                }
            }
            for (Pair<OntologyGlossary, InferenceParse> glossary : glossaries) {
                Ontology.OntologyEnum.Glossary.save(pluginContext, ontologyDomain, glossary.getKey());
            }

            updatedObjectType.forEach((objectTypeName, objType) -> {
                Ontology.OntologyEnum.ObjectType.save(pluginContext, this.ontologyDomain, objType);
            });
        }
    }

    private static <TT extends Ontology> List<Pair<TT, InferenceParse>> createOntologyResources(
            IPluginContext pluginContext, JSONArray ontologyJsonArray, Context ctx) {
        List<Pair<TT, InferenceParse>> ress = ontologyJsonArray.stream().map((o) -> {
            if (o instanceof JSONObject json) {
                AttrValMap valMap = AttrValMap.parseDescribableMap(Optional.empty(),
                        FlatJsonToTisConverter.convert(json));
                //json.getString("")
                InferenceParse inferenceParseResult = InferenceParse.deserialize(json);

                return Pair.of((TT) valMap.createDescribable((IControlMsgHandler) pluginContext, ctx).getInstance(),
                        inferenceParseResult);
            } else {
                throw new IllegalStateException("element must be type of " + JSONObject.class.getName());
            }
        }).toList();
        return ress;
    }


    private <TT extends Describable> TT createDescribable(IPluginContext pluginContext, final Context ctx,
                                                          JSONObject json) {
        AttrValMap valMap = com.qlangtech.tis.util.AttrValMap.parseDescribableMap(Optional.empty(),
                FlatJsonToTisConverter.convert(json));
        return (TT) valMap.createDescribable((IControlMsgHandler) pluginContext, ctx).getInstance();
    }

    private JSONObject buildTablesPayload(List<OntologyObjectType> objectTypes) {
        JSONObject payload = new JSONObject();
        JSONArray tables = new JSONArray();
        for (OntologyObjectType ot : objectTypes) {
            JSONObject tableObj = new JSONObject();
            tableObj.put("name", ot.getName());
            JSONArray columns = new JSONArray();
            for (OntologyProperty col : ot.getCols()) {
                JSONObject colObj = new JSONObject();
                colObj.put("name", col.getName());
                colObj.put("type", col.parseOntologyType().name());
                colObj.put("pk", col.isPk());
                colObj.put("nullable", col.isNullable());
                if (StringUtils.isNotEmpty(col.getDescription())) {
                    colObj.put("comment", col.getDescription());
                }
                columns.add(colObj);
            }
            tableObj.put("columns", columns);
            tables.add(tableObj);
        }
        payload.put("tables", tables);
        return payload;
    }

    //    private String buildSystemPrompt() {
    //        return """
    //                你是一个数据建模专家，擅长分析数据库表结构并推断表之间的语义关系。
    //
    //                根据用户提供的表结构列表（JSON格式），请分析并推断以下本体对象：
    //
    //                ## 1. Link Type（关联关系）
    //                表之间的关联关系，有三种类型：
    //                - ObjectTypeForeignKeys (token=1): 通过外键关联，用于一对一或一对多关系。
    //                  判断依据：某表的列名为 xxx_id 且另一张表名为 xxx 且有 id 主键。
    //                - JoinTableDataset (token=2): 通过中间表关联，用于多对多关系。
    //                  判断依据：某表只有两个外键列组成联合主键，分别指向两张实体表。
    //                - BackingObjectType (token=3): 通过中间对象类型关联，用于带属性的多对多关系。
    //                  判断依据：某表有两个外键列但还有其他业务属性列。
    //
    //                ## 2. Shared Property（共享属性）
    //                多个表中出现的相同语义的属性，适合抽取为共享属性复用。
    //                判断依据：多张表中出现相同名称且相同类型的列（如 create_time, update_time, status, currency_code 等）。
    //                至少在2张表中出现才考虑抽取。
    //
    //                ## 3. Value Type（值类型 + 约束）
    //                列值有明确约束的属性，适合定义为值类型。
    //                判断依据：
    //                - 列注释中包含枚举值列表（如 "PENDING/PAID/SHIPPED"）→ Enum 约束
    //                - 列类型暗示范围约束（如 VARCHAR(3) 可能是国家代码）→ Range 约束
    //
    //                ## 4. Glossary（业务术语 / 同义词词典）
    //                业务术语字典，用于 ChatBI 自然语言到 SQL 的桥接，把用户口语化的业务名词映射到本体对象。
    //                有三种 target 类型：
    //                - GlossaryTargetOT: 业务实体名 → 某个 ObjectType。
    //                  判断依据：表名对应业务实体（如 customer/orders/products）。
    //                  示例：term="客户"，synonyms=["用户","User","buyer","购买方"]，target.targetType="GlossaryTargetOT"，target
    //                  .objectType="customer"
    //                - GlossaryTargetProperty: 业务字段名 → 某个 ObjectType 的某列。
    //                  判断依据：业务上有明确语义的列（如 amount/status/created_at）。
    //                  示例：term="订单金额"，synonyms=["金额","总额","订单总额"]，target.targetType="GlossaryTargetProperty"，target
    //                  .objectType="orders"，target.propertyName="amount"
    //                - GlossaryTargetMetricExpr: 业务指标 → 自定义 SQL 表达式。
    //                  判断依据：常见业务指标（如总销售额、活跃用户数、客单价）可由聚合 SQL 表达。
    //                  示例：term="总销售额"，synonyms=["销售总额","GMV"]，target.targetType="GlossaryTargetMetricExpr"，target
    //                  .sql="SUM(orders.amount)"
    //
    //                同义词请尽量覆盖：中文同义词、英文同义词、口语化表达、行业术语。
    //                优先从列注释/表名中提取业务名词，避免编造。
    //
    //                ## 输出要求
    //                请严格按照 response_format 中定义的 JSON Schema 格式输出。
    //                对于每个推断结果，请给出 confidence 字段（high/medium/low）表示置信度。
    //                - high: 有明确证据（如显式外键命名、注释中的枚举值）
    //                - medium: 基于命名约定推断（如 xxx_id 引用 xxx 表）
    //                - low: 基于经验猜测
    //                """;
    //    }

    private String buildSystemPrompt() {

        return """
                你是一个数据建模专家，擅长分析数据库表结构并推断表之间的语义关系。
                
                根据用户提供的表结构列表（JSON格式），请分析并推断以下本体对象：
                
                ## 1. Link Type（linkTypes）（关联关系）
                表之间的关联关系，有三种类型：
                - Object type foreign keys (token=1): 通过外键关联，用于一对一或一对多关系。
                  判断依据：某表的列名为 xxx_id 且另一张表名为 xxx 且有 id 主键。
                - Join table dataset (token=2): 通过中间表关联，用于多对多关系。
                  判断依据：某表只有两个外键列组成联合主键，分别指向两张实体表。
                - Backing object type (token=3): 通过中间对象类型关联，用于带属性的多对多关系。
                  判断依据：某表有两个外键列但还有其他业务属性列。
                
                ### ⚠️ 核心优先级规则（最高优先级）：通用多重关联识别
                
                在进行任何技术性判定之前，请执行以下通用业务语义匹配逻辑。请务必遵循此逻辑，以确保处理生产环境中各种命名的表：
                
                1.  **通用特征匹配（不再依赖具体词汇）**：
                    *   **判定条件**：如果一张表（记为 Table_J）包含至少两列外键（例如 `ID_A`, `ID_B`），分别指向两张不同的维度表（Table_A, Table_B），**并且** Table_J 中还包含其他非外键的业务属性列（如数值、描述等，即非 `_ID` 结尾的列）。
                    *   **结论**：请无条件推断为 `Backing object type` (token=3)。
                    *   *注意：不要去猜测业务含义（如库存、销售），只要结构符合“双外键+属性”，就是 Backing object type。*
                
                2.  **解除路径占用（关键通用逻辑）**：
                    *   **独立性原则**：**每张表都是独立的分析单元。**
                    *   **逻辑说明**：即使你已经为实体对 (Table_A - Table_B) 创建了一个 `Backing object type` 连接（例如来自表 X），**如果遇到了另一张表 Y，且表 Y 也符合上述“双外键+属性”的特征**，你必须**再次**创建一个新的 `Backing object type` 连接。
                    *   *通俗解释：不要去重。在数据库里，两张不同的表（哪怕它们连接的是相同的两张维度表），也代表了两种不同的物理存储和业务事实。一张表对应一个连接对象。*
                
                3.  **主键忽略（保留原有逻辑）**：
                    *   满足上述特征时，忽略主键（pk）属性的真假，直接视为带属性的多对多关系。
                
                ## 2. Shared Property（sharedProperties，共享属性）
                多个表中出现的相同语义的属性，适合抽取为共享属性复用。
                判断依据：多张表中出现相同名称且相同类型的列（如 create_time, update_time, status, currency_code 等）。
                至少在2张表中出现才考虑抽取。
                
                ## 3. Value Types（valueTypes，值类型 + 约束）
                列值有明确约束的属性，适合定义为值类型。
                判断依据：
                - 列注释中包含枚举值列表（如 "PENDING/PAID/SHIPPED"）→ Enum 约束
                - 列类型暗示范围约束（如 VARCHAR(3) 可能是国家代码）→ Range 约束
                
                ## 4. Glossary（glossaries，业务术语 / 同义词词典）
                业务术语字典，用于 ChatBI 自然语言到 SQL 的桥接，把用户口语化的业务名词映射到本体对象。
                有三种 target 类型：
                - GlossaryTargetOT: 业务实体名 → 某个 ObjectType。
                  判断依据：表名对应业务实体（如 customer/orders/products）。
                  示例：term="客户"，synonyms=["用户","User","buyer","购买方"]，target.targetType="GlossaryTargetOT"，target
                  .objectType="customer"
                - GlossaryTargetProperty: 业务字段名 → 某个 ObjectType 的某列。
                  判断依据：业务上有明确语义的列（如 amount/status/created_at）。
                  示例：term="订单金额"，synonyms=["金额","总额","订单总额"]，target.targetType="GlossaryTargetProperty"，target
                  .objectType="orders"，target.propertyName="amount"
                - GlossaryTargetMetricExpr: 业务指标 → 自定义 SQL 表达式。
                  判断依据：常见业务指标（如总销售额、活跃用户数、客单价）可由聚合 SQL 表达。
                  示例：term="总销售额"，synonyms=["销售总额","GMV"]，target.targetType="GlossaryTargetMetricExpr"，target
                  .sql="SUM(orders.amount)"
                
                同义词请尽量覆盖：中文同义词、英文同义词、口语化表达、行业术语。
                优先从列注释/表名中提取业务名词，避免编造。                    
                
                ## 输出要求
                1. 请严格按照 response_format 中定义的 JSON Schema 格式输出。针对response_format 中 `linkTypes`，`sharedProperties` ，`valueTypes` ，`glossaries` 几种实例内容都请推断并填充。
                2. 请明确区分 response_format 中定义的 JSON Schema 中的`description`字段说明，`description`可以是对输入字段的说明，也可能是必须输入的属性之一，如下schema片段：
                   ```json
                   {
                   	"additionalProperties": false,
                   	"type": "object",
                   	"properties": {
                   	   "name": {
                   		 "pattern": "[A-Z\\\\da-z_\\\\-]+",
                   		 "description": "值类型的唯一标识名称,例子:例如: CountryCode",
                   		  "type": "string"
                   	   },
                   		"description": {
                   			"description": "值类型的详细说明,例子:例如: 国家代码",
                   		    "type": "string"
                   		 }
                   		},
                   		"required": ["name", "description"]
                   	}
                   ```
                   如上定义了两个属性"name"和"description"，两属性都一个"description"进行字段说明，但需要注意 "description"也是需要输入的属性之一，且"required"属性列表中已经注明，必须输入项目，不能遗漏。
                3. 对于每个推断结果，请给出 confidence 字段（high/medium/low）表示置信度。
                   - high: 有明确证据（如显式外键命名、注释中的枚举值）
                   - medium: 基于命名约定推断（如 xxx_id 引用 xxx 表）
                   - low: 基于经验猜测
                """;
    }


    // private

    private TISJsonSchema buildOutputJsonSchema() {
        TISJsonSchema.Builder builder = TISJsonSchema.Builder.create("ontology_inference_result", Optional.empty());

        // linkTypes array
        builder.addProperty(KEY_LINK_TYPES, TISJsonSchema.FieldType.Array, "推断出的关联关系列表")
                .setItems(buildLinkTypeItemSchema());

        // sharedProperties array
        builder.addProperty(KEY_SHARED_PROPERTIES, TISJsonSchema.FieldType.Array, "推断出的共享属性列表")
                .setItems(buildSharedPropertyItemSchema());
        //
        // valueTypes array
        builder.addProperty(KEY_VALUE_TYPES, TISJsonSchema.FieldType.Array, "推断出的值类型列表")
                .setItems(buildValueTypeItemSchema());

        // glossaries array
        builder.addProperty(KEY_GLOSSARIES, TISJsonSchema.FieldType.Array, "推断出的业务术语列表")
                .setItems(buildGlossaryItemSchema());

        return builder.build();
        //        TISJsonSchema schema = ;
        //        StringBuilder buffer = new StringBuilder();
        //        schema.appendFieldDescToPrompt(buffer);
        //        System.out.println(buffer);
        //        return schema;
    }


    public static void main(String[] args) {
        DefaultGroovyShellFactory.setInConsoleModule();
        InferOntologyFromLLM infer = new InferOntologyFromLLM();
        infer.llm = "qwen1";
        String ontologyName = "falcon_14";
        PartialSettedPluginContext pluginContext = IPluginContext.namedContext(ontologyName);

        //
        JSONObject postContent = new JSONObject();
        postContent.put(ManipuldateUtils.KEY_ManipulatePluginMeta,
                ONTOLOGY_DOMAIN.getIdentity() + ":" + UploadPluginMeta.KEY_REQUIRE + "," + NAME_ONTOLOGY_DOMAIN + "_" + ontologyName);
        pluginContext.setPostContent(postContent);
        // pluginContext.setLoginUser()
        pluginContext.setLoginUser(() -> "admin");
        DefaultContext context = new DefaultContext();
        pluginContext.setContext(context);


        //        UploadPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta(UploadPluginMeta.create(Ontology
        //        .ONTOLOGY))
        //                .getDelegate().putExtraParams(NAME_ONTOLOGY_DOMAIN, ontologyName);

        //  context.put(UploadPluginMeta.KEY_PLUGIN_META, pluginMeta);
        // UploadPluginMeta.putPluginMeta(context, pluginMeta);
        //        com.alibaba.fastjson.JSONObject linkType = JsonUtil.loadJSON(InferOntologyFromLLM.class, "test.json");
        //        OntologyLinker linker = infer.createDescribable(pluginContext, context, linkType);

        //        com.alibaba.fastjson.JSONObject content = JsonUtil.loadJSON(InferOntologyFromLLM.class, "test.json");
        //        DeserializeOntologyRes ontologyRes = InferOntologyFromLLM.deserializeOntologyRes(pluginContext,
        //        content,
        //                context);
        //        ontologyRes.create(pluginContext,ontologyName);

        // System.out.println(linker);
        infer.manipuldateProcess(pluginContext, null, Optional.of(context));

        //        TISJsonSchema schema = infer.buildOutputJsonSchema();
        //
        //        System.out.println(JsonUtil.toString(schema.root()));
        //
        //        DescriptorsJSONForAIPrompt descriptorsJSON =
        //                new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new OntologyLinker.DefaultDesc
        //                        ()), true);
        //
        //        DescriptorsMeta descMeta
        //                = descriptorsJSON.getDescriptorsJSON();
        //
        //        for (Map.Entry<String /* concrete plugin implement class */, ITISJsonSchema> entry :
        //                descMeta.getPluginJsonSchema().entrySet()) {
        //
        //            System.out.println(JsonUtil.toString(entry.getValue().root()));
        //        }
    }

    /**
     *
     * @return
     * @see OntologyLinker
     */
    private ITISJsonSchema buildLinkTypeItemSchema() {

        DescriptorsJSONForAIPrompt descriptorsJSON =
                new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologyLinker.DefaultDesc()),
                        true, (b, desc) -> {
                    // 推断元数据
                    InferenceParse.add2SchemaBuilder(b);
                }, (attr, addedProp) -> false);
        DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();
        ITISJsonSchema schema = meta.getFirstPluginJsonSchema();
        return schema;
    }

    /**
     *
     * @return
     * @see OntologySharedProperty
     */
    private ITISJsonSchema buildSharedPropertyItemSchema() {

        DescriptorsJSONForAIPrompt descriptorsJSON =
                new DescriptorsJSONForAIPrompt<>(Collections.singletonList(new DefaultOntologySharedProperty.DefaultDesc()),
                        true
                        , (b, desc) -> {
                    InferenceParse.addTargetColumns2Schema(b);
                    // 推断元数据
                    //                    b.addProperty("confidence", TISJsonSchema.FieldType.String, "置信度")
                    //                            .setValEnums("high", "medium", "low");
                    InferenceParse.add2SchemaBuilder(b);
                }, (attr, addedProp) -> false
                );
        DescriptorsMeta meta = descriptorsJSON.getDescriptorsJSON();
        return meta.getPluginJsonSchema().values().iterator().next();
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
    private ITISJsonSchema buildValueTypeItemSchema() {

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

        //        TISJsonSchema.Builder b = TISJsonSchema.Builder.create("valueTypeItem", Optional.of("valueTypes"));
        //
        //        // 把 host schema 的 properties (impl, vals) 平铺到外层
        //        JSONObject hostProps = hostSchema.schema().getJSONObject(TISJsonSchema.SCHEMA_PROPERTIES);
        //        for (String key : hostProps.keySet()) {
        //            b.addRawProperty(key, hostProps.getJSONObject(key), true);
        //        }
        //
        //
        //        return b.build();
    }

    /**
     * @see OntologyGlossary
     */
    private ITISJsonSchema buildGlossaryItemSchema() {

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
    }

    /**
     * 把 LLM 返回 JSON 中的 primitive 值（Number / Boolean 等）递归包装成
     * <code>{_primaryVal: val}</code>，使其与 {@link com.qlangtech.tis.util.impl.AttrVals#parseAttrValMap}
     * 期望的 TIS 表单格式一致。
     * <p>
     * 必要性：{@code AttrVals#parseAttrValMap} 仅对 {@link String} 值的 cast 失败做自动包装兜底，
     * 对 Integer / Boolean 等会直接抛 RuntimeException。{@link OntologyValueType} 的 step1
     * {@link com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType#type} 是 int，
     * 必现该问题。
     * <p>
     * 适用范围：JSONObject 内部所有非 String / 非容器的 primitive 字段；JSONArray 元素递归处理。
     * String 留给 {@code parseAttrValMap} 的 catch 分支兜底，不动。
     */
    //    static void normalizeValsForReparse(Object node) {
    //        if (node instanceof JSONObject) {
    //            JSONObject obj = (JSONObject) node;
    //            for (String key : new HashSet<>(obj.keySet())) {
    //                Object v = obj.get(key);
    //                if (v instanceof JSONObject || v instanceof JSONArray) {
    //                    normalizeValsForReparse(v);
    //                } else if (v != null && !(v instanceof String)) {
    //                    JSONObject wrapped = new JSONObject();
    //                    wrapped.put(Descriptor.KEY_primaryVal, v);
    //                    obj.put(key, wrapped);
    //                }
    //                // String 由 AttrVals.parseAttrValMap 的 catch 分支自动包装
    //            }
    //        } else if (node instanceof JSONArray) {
    //            JSONArray arr = (JSONArray) node;
    //            for (int i = 0; i < arr.size(); i++) {
    //                normalizeValsForReparse(arr.get(i));
    //            }
    //        }
    //    }


    @TISExtension
    public static final class DftDesc extends OntologyDomainManipulate.BasicDesc implements IEndTypeGetter {
        public DftDesc() {
            super();
            //            List<Pair<OntologyDomain, IPluginStore<OntologyDomain>>> domainList = OntologyDomain
            //            .getDoaminList();
            //            List<OntologyDomain> domains = domainList.stream().map(Pair::getKey).toList();
            //            this.registerSelectOptions("ontologyDomain", () -> domains);
            this.registerSelectOptions(KEY_FIELD_LLM_NAME, LLMProvider::getExistProviders);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            //  return super.validateAll(msgHandler, context, postFormVals);
            OntologyPluginMeta ometa = getOntologyPluginMeta((IPluginContext) msgHandler, Optional.of(context));

            List<OntologyObjectType> objectTypes = OntologyObjectType.loadAll(ometa.getDomain());
            if (objectTypes.isEmpty()) {
                throw new IllegalStateException("domain '" + ometa.getDomain()
                        + "' has no ObjectType, please export tables first");
            }
            Optional<OntologyProperty> pk = null;
            List<OntologyObjectType> lackPkObjTypes = Lists.newArrayList();
            for (OntologyObjectType objType : objectTypes) {
                pk = objType.getPk();
                if (!pk.isPresent()) {
                    lackPkObjTypes.add(objType);
                }
            }

            if (CollectionUtils.isNotEmpty(lackPkObjTypes)) {
                msgHandler.addErrorMessage(context,
                        "对象还未设置主键，完成后继续此操作：" //
                                + lackPkObjTypes.stream() //
                                .map((obj) -> "'" + obj.getName() + "'").collect(Collectors.joining(",")));
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "Infer Ontology From LLM";
        }

        @Override
        public EndType getEndType() {
            return EndType.Ontology;
        }
    }
}

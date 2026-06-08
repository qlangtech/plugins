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

package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.aiagent.llm.FlatJsonToTisConverter;
import com.qlangtech.tis.aiagent.llm.LLMOptionParams;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.aiagent.llm.TISJsonSchema;
import com.qlangtech.tis.datax.job.SSEEventWriter;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.impl.DefaultGroovyShellFactory;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ds.manipulate.ManipuldateUtils;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.StreamingJsonOntologyParser;
import com.qlangtech.tis.plugin.ontology.TargetProperty;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class InferOntologyFromLLMStep1 extends OneStepOfMultiSteps {

    //    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require, Validator.identity})
    //    public String ontologyDomain;
    public static final String KEY_LINK_TYPES = "linkTypes";
    public static final String KEY_SHARED_PROPERTIES = "sharedProperties";
    public static final String KEY_VALUE_TYPES = "valueTypes";
    public static final String KEY_GLOSSARIES = "glossaries";
    private static final Logger logger = LoggerFactory.getLogger(InferOntologyFromLLMStep1.class);
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

    public static OntologyPluginMeta getOntologyPluginMeta(IPluginContext pluginContext, Optional<Context> context) {
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
    protected void processPreSaved(IPluginContext pluginContext, Context ctx, OneStepOfMultiSteps[] preSavedStepPlugins) {

//    }
//
//    @Override
//    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context,
//                                           ManipulateItemsProcessor itemsProcessor) {
        // final Context ctx = context.orElseThrow();

        OntologyPluginMeta ometa = getOntologyPluginMeta(pluginContext, Optional.of(ctx));

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

        LLMOptionParams optParams = new LLMOptionParams();
        optParams.setStreamOutput(true);

        // 使用并发队列收集流式解析结果
        ConcurrentLinkedQueue<Pair<OntologyLinker, InferenceParse>> linkTypesQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Pair<OntologySharedProperty, InferenceParse>> sharedPropsQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Pair<OntologyValueType, InferenceParse>> valueTypesQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Pair<OntologyGlossary, InferenceParse>> glossariesQueue = new ConcurrentLinkedQueue<>();

        // 创建流式解析器
        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        AtomicBoolean hasError = new AtomicBoolean(false);

        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                try {
                    Pair<OntologyLinker, InferenceParse> result = deserializeElement(element, pluginContext, ctx);
                    linkTypesQueue.add(result);
                    logger.info("[Parsed LinkType: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing LinkType: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                try {
                    Pair<OntologySharedProperty, InferenceParse> result = deserializeElement(element, pluginContext, ctx);
                    sharedPropsQueue.add(result);
                    logger.info("[Parsed SharedProperty: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing SharedProperty: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onValueType(JSONObject element) {
                try {
                    Pair<OntologyValueType, InferenceParse> result = deserializeElement(element, pluginContext, ctx);
                    valueTypesQueue.add(result);
                    logger.info("[Parsed ValueType: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing ValueType: " + e.getMessage() + "\njson:\n" + element.toJSONString(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onGlossary(JSONObject element) {
                try {
                    Pair<OntologyGlossary, InferenceParse> result = deserializeElement(element, pluginContext, ctx);
                    glossariesQueue.add(result);
                    logger.info("[Parsed Glossary: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing Glossary: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }
        });


        optParams.setStreamOutputConsumer((reader) -> {
            reader.lines().forEach((line) -> {
                if (StringUtils.isEmpty(line) || "data: [DONE]".equals(line)) {
                    return;
                }
                try {
                    JSONObject data = JSONObject.parseObject(SSEEventWriter.getDataContent(line));
                    if (data == null) {
                        return;
                    }
                    JSONArray choices = data.getJSONArray("choices");
                    for (Object c : choices) {
                        if (c instanceof JSONObject choice) {
                            String content = choice.getJSONObject("delta").getString("content");
                            if (content != null) {
                                System.out.print(content);
                                // 将内容喂给流式解析器
                                parser.appendChunk(content);
                                parser.parse();
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(line, e);
                }
            });

            // 完成解析
            try {
                parser.finish();
            } catch (Exception e) {
                throw new RuntimeException("Error finishing parser", e);
            }
        });
//        /**
//         * 大模型推断
//         */
//        LLMProvider.LLMResponse response = llmProvider.chatJson(
//                IAgentContext.createNull(),
//                new UserPrompt("Infer ontology relations", userPrompt),
//                Collections.singletonList(systemPrompt),
//                buildOutputJsonSchema(), optParams);
//
//        //  System.out.println(parser.buffer);
//
//        if (!response.isSuccess()) {
//            throw new IllegalStateException("LLM inference failed: "
//                    + (response.getErrorMessage() != null ? response.getErrorMessage() : "no response"));
//        }
//
//        if (hasError.get()) {
//            throw new IllegalStateException("Error occurred during streaming deserialization");
//        }
//
//        // 流式模式下从队列构建结果
//        DeserializeOntologyRes ontologyRes = new DeserializeOntologyRes(
//                ometa.getDomain(),
//                new ArrayList<>(linkTypesQueue),
//                new ArrayList<>(sharedPropsQueue),
//                new ArrayList<>(valueTypesQueue),
//                new ArrayList<>(glossariesQueue)
//        );
//
//        ontologyRes.create(pluginContext);
    }

//    public DeserializeOntologyRes deserializeOntologyRes(String ontologyDomain, IPluginContext pluginContext,
//                                                         JSONObject jsonContent
//            , Context ctx) {
//
//        JSONArray linkTypesJsonArray = jsonContent.getJSONArray(KEY_LINK_TYPES);
//        JSONArray sharedPropsJsonArray = jsonContent.getJSONArray(KEY_SHARED_PROPERTIES);
//        JSONArray valueTypesJsonArray = jsonContent.getJSONArray(KEY_VALUE_TYPES);
//        JSONArray glossariesJsonArray = jsonContent.getJSONArray(KEY_GLOSSARIES);
//
//        List<Pair<OntologyLinker, InferenceParse>> linkTypes //
//                = createOntologyResources(pluginContext, linkTypesJsonArray, ctx);
//        List<Pair<OntologySharedProperty, InferenceParse>> sharedProps  //
//                = createOntologyResources(pluginContext, sharedPropsJsonArray, ctx);
//        List<Pair<OntologyValueType, InferenceParse>> valueTypes //
//                = createOntologyResources(pluginContext, valueTypesJsonArray, ctx);
//        List<Pair<OntologyGlossary, InferenceParse>> glossaries //
//                = createOntologyResources(pluginContext, glossariesJsonArray, ctx);
//
//        return new DeserializeOntologyRes(ontologyDomain, linkTypes, sharedProps, valueTypes, glossaries);
//
//    }

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
                List<TargetProperty> targetProperties = inferenceParse.getTargetProps();
                for (TargetProperty targetProperty : targetProperties) {
                    objectType = this.getObjectType(targetProperty);
                    objectType.setSharedProperty(targetProperty, sharedProperty.getKey());
                }
            }
            for (Pair<OntologyValueType, InferenceParse> valueType : valueTypes) {
                Ontology.OntologyEnum.ValueType.save(pluginContext, ontologyDomain, valueType.getKey());
                inferenceParse = valueType.getValue();
                List<TargetProperty> targetProperties = inferenceParse.getTargetProps();
                for (TargetProperty targetProperty : targetProperties) {
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
                Pair<TT, InferenceParse> result = deserializeElement(json, pluginContext, ctx);
                return result;
            } else {
                throw new IllegalStateException("element must be type of " + JSONObject.class.getName());
            }
        }).collect(Collectors.toList());
        return ress;
    }

    /**
     * 反序列化单个本体元素
     * 用于流式解析和批量解析共享逻辑
     */
    static <TT extends Ontology> Pair<TT, InferenceParse> deserializeElement(
            JSONObject json, IPluginContext pluginContext, Context ctx) {
        AttrValMap valMap = AttrValMap.parseDescribableMap(Optional.empty(),
                FlatJsonToTisConverter.convert(json));
        TT ontologyRes = (TT) valMap.createDescribable((IControlMsgHandler) pluginContext, ctx).getInstance();
        InferenceParse inferenceParseResult = InferenceParse.deserialize(json, ontologyRes);

        return Pair.of(ontologyRes, inferenceParseResult);
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

    private static class SystemPrompt4OntologyResource {
        private final String title;
        private final String content;

        public SystemPrompt4OntologyResource(String title, String content) {
            this.title = title;
            this.content = content;
        }
    }

    private String buildSystemPrompt() {

        return """
                你是一个数据建模专家，擅长分析数据库表结构并推断表之间的语义关系。
                
                根据用户提供的表结构列表（JSON格式），请分析并推断以下本体对象：
                
                ## 1. Link Type（linkTypes）（关联关系）
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
                
                ## 📝 最终输出格式 (严格遵守)
                1. 我将通过流式接口接收你的响应。请不要输出任何解释性文字，直接输出 JSON 数据流，请**严格按照response_format中定义的json schema 格式输出**。针对response_format 中 `linkTypes`，`sharedProperties` ，`valueTypes` ，`glossaries` 几种实例内容进行推理。
                
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

//        // linkTypes array
//        builder.addProperty(KEY_LINK_TYPES, TISJsonSchema.FieldType.Array, "推断出的关联关系列表")
//                .setItems(buildLinkTypeItemSchema());
//
//        // sharedProperties array
//        builder.addProperty(KEY_SHARED_PROPERTIES, TISJsonSchema.FieldType.Array, "推断出的共享属性列表")
//                .setItems(buildSharedPropertyItemSchema());
//        //
//        // valueTypes array
//        builder.addProperty(KEY_VALUE_TYPES, TISJsonSchema.FieldType.Array, "推断出的值类型列表")
//                .setItems(buildValueTypeItemSchema());
//
//        // glossaries array
//        builder.addProperty(KEY_GLOSSARIES, TISJsonSchema.FieldType.Array, "推断出的业务术语列表")
//                .setItems(buildGlossaryItemSchema());

        return builder.build();
        //        TISJsonSchema schema = ;
        //        StringBuilder buffer = new StringBuilder();
        //        schema.appendFieldDescToPrompt(buffer);
        //        System.out.println(buffer);
        //        return schema;
    }


    public static void main(String[] args) {
        DefaultGroovyShellFactory.setInConsoleModule();
        InferOntologyFromLLMStep1 infer = new InferOntologyFromLLMStep1();
        infer.llm = "default";// "qwen1";// "qwen1";
        // String ontologyName = "falcon_14";
        String ontologyName = "order2";
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

        infer.manipuldateProcess(pluginContext, null, Optional.of(context));

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
    public static final class DftDesc extends OneStepOfMultiSteps.BasicDesc {
        public DftDesc() {
            super();
            //            List<Pair<OntologyDomain, IPluginStore<OntologyDomain>>> domainList = OntologyDomain
            //            .getDoaminList();
            //            List<OntologyDomain> domains = domainList.stream().map(Pair::getKey).toList();
            //            this.registerSelectOptions("ontologyDomain", () -> domains);
            this.registerSelectOptions(KEY_FIELD_LLM_NAME, LLMProvider::getExistProviders);
        }

        @Override
        public OneStepOfMultiSteps.Step getStep() {
            return Step.Step1;
        }

        @Override
        public Optional<OneStepOfMultiSteps.BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.of(new InferOntologyFromLLMStep2Prompt.DftDesc());
        }

        @Override
        public String getStepDescription() {
            return "基本设置";
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
                if (objType.hasDisablePK()) {
                    continue;
                }
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


    }
}

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
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.aiagent.llm.FlatJsonToTisConverter;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
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
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
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
import java.util.concurrent.Future;
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
//    public static final String KEY_LINK_TYPES = "linkTypes";
//    public static final String KEY_SHARED_PROPERTIES = "sharedProperties";
//    public static final String KEY_VALUE_TYPES = "valueTypes";
//    public static final String KEY_GLOSSARIES = "glossaries";
    private static final Logger logger = LoggerFactory.getLogger(InferOntologyFromLLMStep1.class);
    /**
     * 大模型接口
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, validate = {Validator.identity, Validator.require})
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

        DeserializeOntologyRes ontologyRes
                = DeserializeOntologyRes.getDomainInferResult(ometa.getDomain(), false
                , (inferManager, res) -> {
                    if (res != null && inferManager.remove(ometa.getDomain(), res)) {
                        logger.info("ontology domain:{} relevant inferManager has been remove from register,", ometa.getDomain());
                    }
                    return res;
                });

        if (ontologyRes != null) {
            ontologyRes.stopInferTask(DeserializeOntologyRes.InferBatch.LinkTypeBatch);
            ontologyRes.clearDomainQueues(DeserializeOntologyRes.InferBatch.LinkTypeBatch);

            ontologyRes.stopInferTask(DeserializeOntologyRes.InferBatch.NorLinkTypeBatch);
            ontologyRes.clearDomainQueues(DeserializeOntologyRes.InferBatch.NorLinkTypeBatch);
        }


//        List<OntologyObjectType> objectTypes = OntologyObjectType.loadAll(ometa.getDomain());
//        if (objectTypes.isEmpty()) {
//            throw new IllegalStateException("domain '" + ometa.getDomain()
//                    + "' has no ObjectType, please export tables first");
//        }
//        JSONObject tablesPayload = buildTablesPayload(objectTypes);


        // 流式模式下从队列构建结果
//        DeserializeOntologyRes ontologyRes = new DeserializeOntologyRes(
//                ometa.getDomain(), Objects.requireNonNull(this.getLlmProvider(), "llmProvider can not be null")
//        );

//        ontologyRes.executeInfer(pluginContext, ctx
//                , Pair.of(  OntologyResourceInferenceConfig.glossary )
//                , OntologyResourceInferenceConfig.sharedPropertyConfig
//                , OntologyResourceInferenceConfig.valueType);

        // ontologyRes.create(pluginContext);
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

    //    private static <TT extends Ontology> List<Pair<TT, InferenceParse>> createOntologyResources(
//            IPluginContext pluginContext, JSONArray ontologyJsonArray, Context ctx) {
//        List<Pair<TT, InferenceParse>> ress = ontologyJsonArray.stream().map((o) -> {
//            if (o instanceof JSONObject json) {
//                Pair<TT, InferenceParse> result = deserializeElement(json, pluginContext, ctx);
//                return result;
//            } else {
//                throw new IllegalStateException("element must be type of " + JSONObject.class.getName());
//            }
//        }).collect(Collectors.toList());
//        return ress;
//    }

    /**
     * 反序列化单个本体元素
     * 用于流式解析和批量解析共享逻辑
     */
    static <TT extends Ontology> Pair<TT, InferenceParse> deserializeElement(
            final Integer id, //
            DeserializeOntologyRes.InferBatch inferBatch,
            JSONObject json, IPluginContext pluginContext, Context ctx) {
        AttrValMap valMap = AttrValMap.parseDescribableMap(Optional.empty(),
                FlatJsonToTisConverter.convert(json));
        TT ontologyRes = (TT) valMap.createDescribable((IControlMsgHandler) pluginContext, ctx).getInstance();
        InferenceParse inferenceParseResult = InferenceParse.deserialize(id, inferBatch, json, ontologyRes);
        inferenceParseResult.setSelected(true);

        return Pair.of(ontologyRes, inferenceParseResult);
    }


    private <TT extends Describable> TT createDescribable(IPluginContext pluginContext, final Context ctx,
                                                          JSONObject json) {
        AttrValMap valMap = com.qlangtech.tis.util.AttrValMap.parseDescribableMap(Optional.empty(),
                FlatJsonToTisConverter.convert(json));
        return (TT) valMap.createDescribable((IControlMsgHandler) pluginContext, ctx).getInstance();
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

    public static String ontologyName = "falcon_14";
    // private

    public static PartialSettedPluginContext initPluginContext(String ontologyName) {
        PartialSettedPluginContext pluginContext = IPluginContext.namedContext(ontologyName);
        // pluginContext.setTargetRuntimeContext()
        //

        //UploadPluginMeta.parse()
        JSONObject postContent = new JSONObject();
        postContent.put(ManipuldateUtils.KEY_ManipulatePluginMeta,
                ONTOLOGY_DOMAIN.getIdentity() + ":" + UploadPluginMeta.KEY_REQUIRE + "," + NAME_ONTOLOGY_DOMAIN + "_" + ontologyName);


        pluginContext.setPostContent(postContent);
        // pluginContext.setLoginUser()
        pluginContext.setLoginUser(() -> "admin");
        DefaultContext context = new DefaultContext();
        pluginContext.setContext(context);
        UploadPluginMeta.putPluginMeta(context, UploadPluginMeta.parse(postContent.getString(ManipuldateUtils.KEY_ManipulatePluginMeta)));
        return pluginContext;
    }


    public static void main(String[] args) throws Exception {
        DefaultGroovyShellFactory.setInConsoleModule();
        InferOntologyFromLLMStep1 infer = new InferOntologyFromLLMStep1();
        infer.llm = "Anthropic"; //"default";// "qwen1";// "qwen1";

        //String ontologyName = "order2";
        PartialSettedPluginContext pluginContext = initPluginContext(ontologyName);// IPluginContext.namedContext(ontologyName);
        // pluginContext.setTargetRuntimeContext()
        //

        //UploadPluginMeta.parse()
        //JSONObject postContent = new JSONObject();
//        postContent.put(ManipuldateUtils.KEY_ManipulatePluginMeta,
//                ONTOLOGY_DOMAIN.getIdentity() + ":" + UploadPluginMeta.KEY_REQUIRE + "," + NAME_ONTOLOGY_DOMAIN + "_" + ontologyName);


        // pluginContext.setPostContent(postContent);
        // pluginContext.setLoginUser()
//        pluginContext.setLoginUser(() -> "admin");
        Context context = pluginContext.getContext();// new DefaultContext();
//        pluginContext.setContext(context);
//        UploadPluginMeta.putPluginMeta(context, UploadPluginMeta.parse(postContent.getString(ManipuldateUtils.KEY_ManipulatePluginMeta)));
        // infer.manipuldateProcess(pluginContext, null, Optional.of(context));

        InferOntologyFromLLMStep2Prompt step2Prompt = new InferOntologyFromLLMStep2Prompt();

        step2Prompt.glossaryPrompt = OntologyResourceInferenceConfig.glossary.getPrompt();
        step2Prompt.valueTypePrompt = OntologyResourceInferenceConfig.valueType.getPrompt();
        step2Prompt.sharedPropertyPrompt = OntologyResourceInferenceConfig.sharedPropertyConfig.getPrompt();

        //  DeserializeOntologyRes.getOntologyResInfer(ontologyName, pluginContext, context, step2Prompt, infer);


        InferOntologyFromLLMStep3Prompt step3Prompt = new InferOntologyFromLLMStep3Prompt();
        step3Prompt.linkTypePrompt = OntologyResourceInferenceConfig.linkerType.getPrompt();

        Future<?> future = DeserializeOntologyRes.getOntologyResInfer(ontologyName, pluginContext, context, step3Prompt, infer);


        DeserializeOntologyRes ontologyRes
                = DeserializeOntologyRes.getDomainInferResult(ontologyName);
        ontologyRes.subscribe(new InferenceParseSubscriber(DeserializeOntologyRes.InferBatch.LinkTypeBatch) {
            @Override
            public void onNext(InferenceParse item) {
                // super.onNext(item);
                System.out.println(item.getName());
            }
        });

        future.get();
        ConcurrentLinkedQueue<Pair<OntologyLinker, InferenceParse>> linkTypesQueue = ontologyRes.linkTypesQueue;


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
    public static final class DftDesc extends OneStepOfMultiSteps.BasicDesc implements FormFieldType.IMultiSelectValidator {
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

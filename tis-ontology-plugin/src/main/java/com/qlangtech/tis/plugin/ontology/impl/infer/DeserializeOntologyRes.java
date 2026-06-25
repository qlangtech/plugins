package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.aiagent.core.IAgentContext;
import com.qlangtech.tis.aiagent.llm.LLMOptionParams;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.aiagent.llm.TISJsonSchema;
import com.qlangtech.tis.aiagent.llm.UserPrompt;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.llm.impl.qwen.sampling.TemperatureSampling;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyGlossary;
import com.qlangtech.tis.plugin.ontology.OntologyLinker;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.StreamingJsonOntologyParser;
import com.qlangtech.tis.plugin.ontology.TargetProperty;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/9
 */
@SuppressWarnings("all")
public class DeserializeOntologyRes {

    private static final Logger logger = LoggerFactory.getLogger(DeserializeOntologyRes.class);
    private static final ConcurrentMap<String, DeserializeOntologyRes> ontologyResInferManager = Maps.newConcurrentMap();
    final ConcurrentLinkedQueue<Pair<OntologyLinker, InferenceParse>> linkTypesQueue;
    private final ConcurrentLinkedQueue<Pair<OntologySharedProperty, InferenceParse>> sharedPropsQueue;
    private final ConcurrentLinkedQueue<Pair<OntologyValueType, InferenceParse>> valueTypesQueue;
    private final ConcurrentLinkedQueue<Pair<OntologyGlossary, InferenceParse>> glossariesQueue;

    private final ConcurrentLinkedQueue<InferenceParse> allResQueue = new ConcurrentLinkedQueue<>();

    private final String ontologyDomain;


    private final AtomicBoolean linkerReferSignal = new AtomicBoolean();
    private final AtomicBoolean otherReferSignal = new AtomicBoolean();
    private final InferBatchExecuteResult otherReferSignalResult = new InferBatchExecuteResult();
    private final InferBatchExecuteResult linkerReferSignalResult = new InferBatchExecuteResult();


    private final ConcurrentMap<String, OntologyObjectType> updatedObjectType = Maps.newConcurrentMap();


    private final JSONObject tablesPayload;
    private final LLMProvider llmProvider;
    private final AtomicInteger idIndex = new AtomicInteger();
    private final SubmissionPublisher<InferenceParse> publisher;
    private final List<String> targetObjectTypes;

    public DeserializeOntologyRes(String ontologyDomain, List<IdentityName> targetObjectTypes, LLMProvider llmProvider) {
        this.linkTypesQueue = new ConcurrentLinkedQueue<>();
        this.sharedPropsQueue = new ConcurrentLinkedQueue<>();
        this.valueTypesQueue = new ConcurrentLinkedQueue<>();
        this.glossariesQueue = new ConcurrentLinkedQueue<>();
        this.ontologyDomain = ontologyDomain;
        this.llmProvider = llmProvider;
        List<OntologyObjectType> objectTypes = OntologyObjectType.loadAll(ontologyDomain);
        if (objectTypes.isEmpty()) {
            throw new IllegalStateException("domain '" + ontologyDomain
                    + "' has no ObjectType, please export tables first");
        }
        this.targetObjectTypes = Objects.requireNonNull(targetObjectTypes) //
                .stream().map((name) -> name.identityValue()).toList();
        this.publisher = new SubmissionPublisher<>();
        this.tablesPayload = buildTablesPayload(targetObjectTypes, objectTypes);
    }

    /**
     * 查找推理实体
     *
     * @param id
     * @return
     */
    public InferenceParse findInferenceById(Integer id) {
        if (id == null) {
            throw new IllegalArgumentException("param id can not be null");
        }
        return allResQueue.stream().filter((infer) -> id.equals(infer.getId()))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "can not find " + InferenceParse.class.getSimpleName() + " with relevant id:" + id));
    }

    public void subscribe(InferenceParseSubscriber subscriber) {
        switch (subscriber.getInferBatch()) {
            case LinkTypeBatch -> {
                if (linkerReferSignalResult.isComplete()) {
                    subscriber.onComplete();
                    logger.info("LinkType infer has complete");
                    return;
                }
            }
            case NorLinkTypeBatch -> {
                if (otherReferSignalResult.isComplete()) {
                    subscriber.onComplete();
                    logger.info(InferBatch.NorLinkTypeBatch + " infer has complete");
                    return;
                }
            }
            default -> {
                throw new IllegalStateException("illegal infer batch type:" + subscriber.getInferBatch());
            }
        }
        this.publisher.subscribe(subscriber);
    }

    public static DeserializeOntologyRes getDomainInferResult(String ontologyDomain) {
        return getDomainInferResult(ontologyDomain, true //
                , (inferManager, res) -> res);
    }

    public static DeserializeOntologyRes getDomainInferResult(String ontologyDomain, boolean validateNull //
            , BiFunction<ConcurrentMap<String, DeserializeOntologyRes>, DeserializeOntologyRes, DeserializeOntologyRes> callback) {
        if (StringUtils.isEmpty(ontologyDomain)) {
            throw new IllegalArgumentException("param ontologyDomain can not be empty");
        }
        DeserializeOntologyRes res = ontologyResInferManager.get(ontologyDomain);
        if (validateNull) {
            Objects.requireNonNull(res
                    , "ontologyDomain:" + ontologyDomain + " relevant DeserializeOntologyRes can not be null");
        }
        return callback.apply(ontologyResInferManager, res);
    }

    public List<InferenceParse> getTargetInferenceParseResult(Set<Ontology.OntologyEnum> filterCriteria) {
        return allResQueue.stream().filter((ip) -> filterCriteria.contains(ip.ontologyEnum())).toList();
    }

    private static final ExecutorService inferExecutor = Executors.newCachedThreadPool();
    // private final
    /**
     * 保存各 batch 正在运行的 Future，供 stopInferTask 取消
     */
    private final ConcurrentMap<InferBatch, Future<?>> runningFutures = Maps.newConcurrentMap();
    /**
     * 每个 batch 的取消标志，用于在不可中断的阻塞 I/O 场景下安全停止流处理
     */
    private final ConcurrentMap<InferBatch, AtomicBoolean> cancelFlags = Maps.newConcurrentMap();

    public static void getOntologyResInfer(String domain, IPluginContext pluginContext, Context ctx //
            , InferOntologyFromLLMStep2Prompt step2Prompt, InferOntologyFromLLMStep1 step1) {
        DeserializeOntologyRes ontologyRes = getDeserializeOntologyRes(domain, step1);
        InferBatch batch = InferBatch.NorLinkTypeBatch;
        logger.info("try to start {} infer,object type size:{}，names：{}" //
                , batch, ontologyRes.targetObjectTypes.size(), String.join(",", ontologyRes.targetObjectTypes));
        if (ontologyRes.otherReferSignal.compareAndSet(false, true)) {
            logger.info("get start {} infer", batch);
            ontologyRes.otherReferSignalResult.setComplete(false);
            ontologyRes.otherReferSignalResult.setFaild(false);
            Object context = pluginContext.getContext().getContext();

            Future<?> future = inferExecutor.submit(() -> {
                boolean falid = true;

                try {
                    IPluginContext.setPluginContext(pluginContext);
                    pluginContext.getContext().setContext(context);
                    ontologyRes.executeInfer(batch, pluginContext, ctx
                            , Pair.of(OntologyResourceInferenceConfig.glossary, step2Prompt.glossaryPrompt)
                            , Pair.of(OntologyResourceInferenceConfig.sharedPropertyConfig, step2Prompt.sharedPropertyPrompt)
                            , Pair.of(OntologyResourceInferenceConfig.valueType, step2Prompt.valueTypePrompt));
                    falid = false;
                } finally {
                    ontologyRes.runningFutures.remove(InferBatch.NorLinkTypeBatch);
                    ontologyRes.notifyBatchComplete(batch);
                    ontologyRes.otherReferSignalResult.setComplete(true);
                    ontologyRes.otherReferSignalResult.setFaild(falid);
                    // publisher.close();
                }
            });
            ontologyRes.runningFutures.put(batch, future);
        } else {
            logger.info("has not acquire the start lock for {} infer", batch);
        }
    }

    public static Future<?> getOntologyResInfer(String domain, IPluginContext pluginContext, Context ctx //
            , InferOntologyFromLLMStep3Prompt step3Prompt, InferOntologyFromLLMStep1 step1) {
        DeserializeOntologyRes ontologyRes = getDeserializeOntologyRes(domain, step1);
        InferBatch batch = InferBatch.LinkTypeBatch;

        logger.info("try to start {} infer,object type size:{}，names:{}", batch
                , ontologyRes.targetObjectTypes.size(), String.join(",", ontologyRes.targetObjectTypes));
        if (ontologyRes.linkerReferSignal.compareAndSet(false, true)) {
            logger.info("get start {} infer", batch);
            Object context = pluginContext.getContext().getContext();

            Future<?> future = inferExecutor.submit(() -> {
                ontologyRes.linkerReferSignalResult.setComplete(false);
                ontologyRes.linkerReferSignalResult.setFaild(false);
                boolean falid = true;

                try {
                    IPluginContext.setPluginContext(pluginContext);
                    pluginContext.getContext().setContext(context);
                    ontologyRes.executeInfer(batch, pluginContext, ctx
                            , Pair.of(OntologyResourceInferenceConfig.linkerType, step3Prompt.linkTypePrompt));
                    falid = false;
                } finally {
                    ontologyRes.runningFutures.remove(InferBatch.LinkTypeBatch);
                    ontologyRes.notifyBatchComplete(batch);
                    ontologyRes.linkerReferSignalResult.setComplete(true);
                    ontologyRes.linkerReferSignalResult.setFaild(falid);
                }
            });
            ontologyRes.runningFutures.put(batch, future);
            return future;
        } else {
            logger.info("has not acquire the start lock for {} infer", batch);
        }
        return null;
    }

    /**
     * 终止正在执行的的推理任务，并将相关状态恢复到初始化状态，以便下次还能重新执行
     */
    public void stopInferTask(InferBatch batch) {
        // 1. 先设置取消标志，让 executeInfer 中的流处理尽快退出
        AtomicBoolean cancelFlag = cancelFlags.get(batch);
        if (cancelFlag != null) {
            cancelFlag.set(true);
            logger.info("stopInferTask: set cancelFlag for batch={}", batch);
        }
        // 2. 取消正在运行的 Future（同时发送线程中断信号作为备用手段）
        Future<?> future = runningFutures.remove(batch);
        if (future != null) {
            future.cancel(true);
            logger.info("stopInferTask: cancelled running future for batch={}", batch);
            // 3. 等待老任务真正结束，最多等待 30 秒，避免新旧任务并发执行
            try {
                future.get(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (java.util.concurrent.CancellationException e) {
                // 正常取消，忽略
            } catch (java.util.concurrent.TimeoutException e) {
                logger.warn("stopInferTask: timeout waiting for batch={} to stop", batch);
            } catch (Exception e) {
                logger.warn("stopInferTask: exception while waiting for batch={} to stop: {}", batch, e.getMessage());
            }
        }
        // 4. 清空对应 batch 的队列数据并重置信号
        switch (batch) {
            case NorLinkTypeBatch -> {
                // 重置信号，以便下次能重新执行
                // otherReferSignal.set(false);
                otherReferSignalResult.setComplete(false);
                otherReferSignalResult.setFaild(false);
            }
            case LinkTypeBatch -> {
                // linkerReferSignal.set(false);
                linkerReferSignalResult.setComplete(false);
                linkerReferSignalResult.setFaild(false);
            }
        }
        // 5. 清理取消标志（仅当仍是本次任务对应的标志时才移除，避免误删新任务刚注册的标志，防止竞态）
        if (cancelFlag != null) {
            cancelFlags.remove(batch, cancelFlag);
        }
        // 不需要，虽然停止了，之前推理结果还继续保留：allResQueue.removeIf(ip -> ip.getInferBatch() == batch);
        logger.info("stopInferTask: state reset for batch={}, queues cleared", batch);
    }

    /**
     * 清除推理域相关队列，供"重新推理"时使用
     */
    public void clearDomainQueues(InferBatch batch) {
        switch (batch) {
            case NorLinkTypeBatch -> {
                sharedPropsQueue.clear();
                valueTypesQueue.clear();
                glossariesQueue.clear();
                this.otherReferSignal.compareAndSet(true, false);
            }
            case LinkTypeBatch -> {
                linkTypesQueue.clear();
                this.linkerReferSignal.compareAndSet(true, false);
            }
        }
        allResQueue.removeIf(ip -> ip.getInferBatch() == batch);
        logger.info("clearDomainQueues: cleared queues for batch={}", batch);
    }


    private void notifyBatchComplete(InferBatch batch) {
        for (Flow.Subscriber s : this.publisher.getSubscribers()) {
            if (s instanceof InferenceParseSubscriber subscriber) {
                if (subscriber.getInferBatch() == batch) {
                    subscriber.onComplete();
                }
            }
        }
    }


    private static DeserializeOntologyRes getDeserializeOntologyRes(String domain, InferOntologyFromLLMStep1 step1) {
        DeserializeOntologyRes ontologyRes = ontologyResInferManager.computeIfAbsent(domain, (d) -> {

            // 流式模式下从队列构建结果
            return new DeserializeOntologyRes(
                    domain, step1.targetTables, Objects.requireNonNull(step1.getLlmProvider(), "llmProvider can not be null")
            );
        });
        return ontologyRes;
    }

    private static JSONObject buildTablesPayload(List<IdentityName> targetObjectTypes, List<OntologyObjectType> objectTypes) {
        JSONObject payload = new JSONObject();
        JSONArray tables = new JSONArray();
        Set<String> acceptObjTypes = targetObjectTypes.stream() //
                .map((name) -> name.identityValue())//
                .collect(Collectors.toSet());

        for (OntologyObjectType ot : objectTypes) {
            if (!acceptObjTypes.contains(ot.getName())) {
                continue;
            }
            JSONObject tableObj = new JSONObject();
            tableObj.put("name", ot.getName());
            JSONArray columns = new JSONArray();
            for (OntologyProperty col : ot.getCols()) {
                JSONObject colObj = new JSONObject();
                colObj.put("name", col.getName());
                colObj.put("type", col.parseOntologyType().name());
                colObj.put("pk", col.isPk());
                colObj.put(OntologyProperty.FIELD_NULLABLE, col.isNullable());
                if (StringUtils.isNotEmpty(col.getDescription())) {
                    colObj.put(Option.KEY_COMMENT, col.getDescription());
                }
                columns.add(colObj);
            }
            tableObj.put("columns", columns);
            tables.add(tableObj);
        }
        if (tables.size() < 1) {
            throw new IllegalStateException("tables can not be empty");
        }
        payload.put("tables", tables);
        return payload;
    }

    private OntologyObjectType getObjectType(final TargetProperty targetProperty) {
        if (targetProperty == null) {
            throw new IllegalArgumentException("param objTypeName can not be empty");
        }
        return updatedObjectType.computeIfAbsent(targetProperty.objectType(), (key) -> {
            return Ontology.loadObjectTypeDetail(ontologyDomain, targetProperty.objectType());
        });
    }

    public int create(Set<Integer> skipIds, IPluginContext pluginContext) {
        InferenceParse inferenceParse = null;
        int createResCount = 0;
        int skipCount = 0;
        OntologyObjectType objectType = null;
        for (Pair<OntologyLinker, InferenceParse> linker : linkTypesQueue) {
            if (skipIds.contains(linker.getValue().getId())) {
                skipCount++;
                continue;
            }
            Ontology.OntologyEnum.Linker.save(pluginContext, ontologyDomain, linker.getKey());
            createResCount++;
        }
        for (Pair<OntologySharedProperty, InferenceParse> sharedProperty : sharedPropsQueue) {
            if (skipIds.contains(sharedProperty.getValue().getId())) {
                skipCount++;
                continue;
            }
            Ontology.OntologyEnum.SharedProperty.save(pluginContext, ontologyDomain, sharedProperty.getKey());
            inferenceParse = sharedProperty.getValue();
            List<TargetProperty> targetProperties = inferenceParse.getTargetProps();
            for (TargetProperty targetProperty : targetProperties) {
                objectType = this.getObjectType(targetProperty);
                objectType.setSharedProperty(targetProperty, sharedProperty.getKey());
            }
            createResCount++;
        }
        for (Pair<OntologyValueType, InferenceParse> valueType : valueTypesQueue) {
            if (skipIds.contains(valueType.getValue().getId())) {
                skipCount++;
                continue;
            }
            Ontology.OntologyEnum.ValueType.save(pluginContext, ontologyDomain, valueType.getKey());
            inferenceParse = valueType.getValue();
            List<TargetProperty> targetProperties = inferenceParse.getTargetProps();
            for (TargetProperty targetProperty : targetProperties) {
                objectType = this.getObjectType(targetProperty);
                objectType.setValeType(targetProperty, valueType.getKey());
            }
            createResCount++;
        }
        for (Pair<OntologyGlossary, InferenceParse> glossary : glossariesQueue) {
            if (skipIds.contains(glossary.getValue().getId())) {
                skipCount++;
                continue;
            }
            Ontology.OntologyEnum.Glossary.save(pluginContext, ontologyDomain, glossary.getKey());
            createResCount++;
        }

        updatedObjectType.forEach((objectTypeName, objType) -> {
            Ontology.OntologyEnum.ObjectType.save(pluginContext, this.ontologyDomain, objType);
        });
        logger.info("create ontology resource count:{},skip count:{}", createResCount, skipCount);
        // 最后需要将ontologyDomain将对应的注册实例删除掉
        ontologyResInferManager.remove(this.ontologyDomain);
        return createResCount;
    }


    private TISJsonSchema buildOutputJsonSchema(OntologyResourceInferenceConfig... inferenceCfgs) {
        TISJsonSchema.Builder builder = TISJsonSchema.Builder.create("ontology_inference_result", Optional.empty());
        for (OntologyResourceInferenceConfig cfg : Objects.requireNonNull(inferenceCfgs, "inferenceCfgs can not be null")) {
            builder.addProperty(cfg.getInferenceType(), TISJsonSchema.FieldType.Array, cfg.getDescription())
                    .setItems(cfg.getJsonSchema());
        }
        return builder.build();
    }

    public enum InferBatch {
        LinkTypeBatch, NorLinkTypeBatch
    }

    private static class InferBatchExecuteResult {
        private boolean complete;
        private boolean faild;
        // private String errorMessage;

        public boolean isComplete() {
            return complete;
        }

        public void setComplete(boolean complete) {
            this.complete = complete;
        }

        public boolean isFaild() {
            return faild;
        }

        public void setFaild(boolean faild) {
            this.faild = faild;
        }

//        public String getErrorMessage() {
//            return errorMessage;
//        }
//
//        public void setErrorMessage(String errorMessage) {
//            this.errorMessage = errorMessage;
//        }
    }

    /**
     * 开始推理生成对应本体资源
     */
    public void executeInfer(InferBatch inferBatch, IPluginContext pluginContext, Context ctx, Pair<OntologyResourceInferenceConfig, String>... inferenceCfgs) {
        final String systemPrompt = this.buildSystemPrompt(inferenceCfgs);
        final String userPrompt = tablesPayload.toJSONString();
        logger.info("start " + inferBatch + " inference");
        // 为本次执行注册一个新的取消标志
        final AtomicBoolean cancelFlag = new AtomicBoolean(false);
        cancelFlags.put(inferBatch, cancelFlag);

        // LLMProvider llmProvider = this.getLlmProvider();

        LLMOptionParams optParams = new LLMOptionParams();
        optParams.setStreamOutput(true);
        // 设置随机性为0
        TemperatureSampling temperature = new TemperatureSampling();
        temperature.temperature = 0f;
        optParams.setSampling(temperature);


        // 创建流式解析器
        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        AtomicBoolean hasError = new AtomicBoolean(false);

        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                try {
                    Pair<OntologyLinker, InferenceParse> result
                            = InferOntologyFromLLMStep1.deserializeElement(idIndex.getAndIncrement(), inferBatch, element, pluginContext, ctx);
                    linkTypesQueue.add(result);
                    allResQueue.add(result.getValue());
                    publisher.submit(result.getValue());
                    logger.info("[Parsed LinkType: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing LinkType: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                try {
                    Pair<OntologySharedProperty, InferenceParse> result
                            = InferOntologyFromLLMStep1.deserializeElement(idIndex.getAndIncrement(), inferBatch, element, pluginContext, ctx);
                    sharedPropsQueue.add(result);
                    allResQueue.add(result.getValue());
                    publisher.submit(result.getValue());
                    logger.info("[Parsed SharedProperty: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing SharedProperty: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onValueType(JSONObject element) {
                try {
                    Pair<OntologyValueType, InferenceParse> result
                            = InferOntologyFromLLMStep1.deserializeElement(idIndex.getAndIncrement(), inferBatch, element, pluginContext, ctx);
                    valueTypesQueue.add(result);
                    allResQueue.add(result.getValue());
                    publisher.submit(result.getValue());
                    logger.info("[Parsed ValueType: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing ValueType: " + e.getMessage() + "\njson:\n" + element.toJSONString(), e);
                    hasError.set(true);
                }
            }

            @Override
            public void onGlossary(JSONObject element) {
                try {
                    Pair<OntologyGlossary, InferenceParse> result
                            = InferOntologyFromLLMStep1.deserializeElement(idIndex.getAndIncrement(), inferBatch, element, pluginContext, ctx);
                    glossariesQueue.add(result);
                    allResQueue.add(result.getValue());
                    publisher.submit(result.getValue());
                    logger.info("[Parsed Glossary: " + result.getKey().identityValue() + "]");
                } catch (Exception e) {
                    logger.warn("Error parsing Glossary: " + e.getMessage(), e);
                    hasError.set(true);
                }
            }
        });


        optParams.setStreamOutputConsumer((line) -> {
            //   reader.lines().forEach((line) -> {
            // 优先检查取消标志（对阻塞 I/O 友好，不依赖线程中断）
            if (cancelFlag.get()) {
                logger.info("executeInfer: cancelFlag set, stopping stream processing for batch={}", inferBatch);
                throw new RuntimeException(new InterruptedException("infer task cancelled by cancelFlag"));
            }
            // 备用：检查线程中断信号（由 stopInferTask cancel(true) 触发）
            if (Thread.currentThread().isInterrupted()) {
                logger.info("executeInfer: thread interrupted, stopping stream processing");
                throw new RuntimeException(new InterruptedException("infer task cancelled"));
            }
            if (StringUtils.isEmpty(line) || "data: [DONE]".equals(line)) {
                return;
            }


            try {
                // System.out.print(line);
                parser.appendChunk(line);
                parser.parse();
//                JSONObject data = JSONObject.parseObject(SSEEventWriter.getDataContent(line));
//                if (data == null) {
//                    return;
//                }
//                JSONArray choices = data.getJSONArray("choices");
//                for (Object c : choices) {
//                    if (c instanceof JSONObject choice) {
//                        String content = choice.getJSONObject("delta").getString("content");
//                        if (content != null) {
//                            // System.out.print(content);
//                            // 将内容喂给流式解析器
//                            parser.appendChunk(content);
//                            parser.parse();
//                        }
//                    }
//                }
            } catch (Exception e) {
                throw new RuntimeException(line, e);
            }
            //}
            // );

            // 完成解析
            try {
                parser.finish();
            } catch (Exception e) {
                throw new RuntimeException("Error finishing parser", e);
            }
        });
        /**
         * 大模型推断
         */
        LLMProvider.LLMResponse response = llmProvider.chatJson(
                IAgentContext.createNull(),
                new UserPrompt("Infer ontology relations", userPrompt),
                Collections.singletonList(systemPrompt),
                buildOutputJsonSchema(Arrays.stream(inferenceCfgs).map(Pair::getKey).toArray(OntologyResourceInferenceConfig[]::new)), optParams);

        //  System.out.println(parser.buffer);
        logger.info(inferBatch + " inference complete,llm response:{},hasError:{}", response.isSuccess(), hasError.get());
        if (!response.isSuccess()) {
            throw new IllegalStateException("LLM inference failed: "
                    + (response.getErrorMessage() != null ? response.getErrorMessage() : "no response"));
        }

        if (hasError.get()) {
            throw new IllegalStateException("Error occurred during streaming deserialization");
        }
    }

    String buildSystemPrompt(Pair<OntologyResourceInferenceConfig, String>... inferenceCfgs) {

        StringBuilder promptBuilder = new StringBuilder();
        promptBuilder.append(
                """
                        你是一个数据建模专家，擅长分析数据库表结构并推断表之间的语义关系。
                        
                        根据用户提供的表结构列表（JSON格式），请分析并推断以下本体对象：
                        """);
        int serNum = 1;
        for (Pair<OntologyResourceInferenceConfig, String> pair : inferenceCfgs) {
            if (StringUtils.isEmpty(pair.getValue())) {
                throw new IllegalStateException("pair.getValue() can not be empty");
            }
            OntologyResourceInferenceConfig cfg = pair.getKey();
            promptBuilder.append("## ").append(serNum++).append(". ").append(cfg.getDescription()).append("\n");
            promptBuilder.append(pair.getValue());
            promptBuilder.append("\n\n");
        }


        promptBuilder.append(
                """ 
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
                        """.stripIndent());
        return promptBuilder.toString();


    }
}

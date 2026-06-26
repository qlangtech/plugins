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
package com.qlangtech.tis.plugin.ontology.chatbi;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.aiagent.core.IAgentContext;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.aiagent.llm.UserPrompt;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.chatbi.prompt.PromptBuilder;
import com.qlangtech.tis.plugin.ontology.chatbi.trace.TraceWriter;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.AstValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.ExplainValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.KeywordWhitelistValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.ValidationResult;
import com.qlangtech.tis.plugin.ontology.graphrag.DefaultGraphRAGService;
import com.qlangtech.tis.plugin.ontology.graphrag.GraphRAGService;
import com.qlangtech.tis.plugin.ontology.graphrag.LinkerInfo;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * ChatBI 服务默认实现（§5 T5：重试编排）。
 * <p>
 * 流水线：检索 → 拼装 Prompt → LLM 调用 → 静态校验 → EXPLAIN 校验（可选）→ 执行（可选）。
 * <p>
 * 每个 ontology 域对应一个独立实例，由 {@link com.qlangtech.tis.plugin.ontology.EnableChatBI} 持有并懒加载。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class DefaultChatBIService implements ChatBIService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultChatBIService.class);

//    private static final DateTimeFormatter DATETIME_FORMATTER =
//            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final GraphRAGService graphRAGService = DefaultGraphRAGService.getInstance();
    private final KeywordWhitelistValidator keywordValidator = new KeywordWhitelistValidator();
    private final AstValidator astValidator = new AstValidator();

    private LLMProvider llmProvider;

    // 配置对象（从 EnableChatBI 注入）
    private com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig retryConfig;
    private com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig validationConfig;
    private com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig executionConfig;
    private com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig retrievalConfig;

    public DefaultChatBIService() {
    }

    public void setLlmProvider(LLMProvider llmProvider) {
        this.llmProvider = Objects.requireNonNull(llmProvider, "param llmProvider can not be null");
    }

    public void setConfigs(com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig retryConfig,
                           com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig validationConfig,
                           com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig executionConfig,
                           com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig retrievalConfig) {
        this.retryConfig = Objects.requireNonNull(retryConfig, "retryConfig can not be null");
        this.validationConfig = Objects.requireNonNull(validationConfig, "validationConfig can not be null");
        this.executionConfig = Objects.requireNonNull(executionConfig, "executionConfig can not be null");
        this.retrievalConfig = Objects.requireNonNull(retrievalConfig, "retrievalConfig can not be null");
    }


    @Override
    public ChatBIResult ask(String domain, String nlq, boolean forceQueryExecute, java.util.function.Consumer<TraceStep> stepCallback) {

        if (forceQueryExecute && !this.executionConfig.isExecuteQuery()) {
            throw new IllegalStateException("ExecuteQuery is not enable, please switch it on. in ontology domain:" + domain);
        }

        // 请求 ID：yyyyMMddHHmmss-{uuid32}，贯穿整个流水线，用作 trace 文件名
        String createTime = TimeFormat.yyyyMMddHHmmss.format(new Date());// LocalDateTime.now().format(DATETIME_FORMATTER);
        String reqId = createTime + "-" + UUID.randomUUID().toString().replace("-", "");

        List<TraceStep> trace = new ArrayList<>();

        try {
            // Step 1: 检索
            long t1 = System.currentTimeMillis();
            com.qlangtech.tis.plugin.ontology.graphrag.RetrievalOptions retrievalOptions =
                    retrievalConfig != null ? retrievalConfig.toRetrievalOptions()
                            : com.qlangtech.tis.plugin.ontology.graphrag.RetrievalOptions.defaults();
            final RetrievalResult retrievalResult = graphRAGService.retrieve(domain, nlq, retrievalOptions);
            long t2 = System.currentTimeMillis();
            Set<String> linkers = retrievalResult.linkers().stream().map(LinkerInfo::linkerName).collect(Collectors.toSet());
            TraceStep retrieveStep = TraceStep.retrieve(retrievalResult.objectTypes().size(), linkers.size(), t2 - t1);
            JSONObject data = Objects.requireNonNull(retrieveStep.data(), "data can not be null");
            data.put("ots", String.join(",", retrievalResult.objectTypes()));
            data.put("linkers", String.join(",", linkers));
            data.put("glossaries", String.join(",", retrievalResult.glossaryTerms()));
            trace.add(retrieveStep);
            stepCallback.accept(retrieveStep);

            if (retrievalResult.objectTypes().isEmpty()) {
                String error = "No relevant ontology found for query: " + nlq;
                TraceStep errStep = TraceStep.error("retrieve", error, null);
                trace.add(errStep);
                stepCallback.accept(errStep);
                TraceWriter.writeTrace(domain, nlq, trace, reqId);
                return ChatBIResult.fail(error, trace, reqId);
            }

            // Step 2: 拼装 Prompt
            String graphragContext = retrievalResult.promptContext();
            List<String> systemPrompt = PromptBuilder.buildSystemPrompt(graphragContext);
            UserPrompt userPrompt = PromptBuilder.buildInitialPrompt(nlq, graphragContext);
            int tokens = PromptBuilder.estimateTokens(systemPrompt.get(0) + userPrompt.getPrompt());
            TraceStep promptStep = TraceStep.prompt(tokens, systemPrompt.get(0), userPrompt.getPrompt());
            trace.add(promptStep);
            stepCallback.accept(promptStep);

            // Step 3-5: LLM 调用 + 校验 + 重试循环
            String candidateSql = null;
            ValidationResult validationResult = null;
            int attempt = 0;
            int maxRetry = retryConfig != null ? retryConfig.getMaxRetry() : 2;
            boolean enableExplain = validationConfig != null && validationConfig.isEnableExplain();

            while (attempt <= maxRetry) {
                attempt++;

                // Step 3: LLM 调用
                long t3 = System.currentTimeMillis();
                LLMProvider.LLMResponse llmResponse = invokeLLM(domain, userPrompt, systemPrompt);
                long t4 = System.currentTimeMillis();

                if (!llmResponse.isSuccess()) {
                    String error = "LLM invocation failed: " + llmResponse.getErrorMessage();
                    TraceStep llmErrStep = TraceStep.error("llm", error, null);
                    trace.add(llmErrStep);
                    stepCallback.accept(llmErrStep);
                    TraceWriter.writeTrace(domain, nlq, trace, reqId);
                    return ChatBIResult.fail(error, trace, reqId);
                }

                TraceStep llmStep = TraceStep.llm(llmResponse.getModel(), llmResponse.getPromptTokens(),
                        llmResponse.getCompletionTokens(), llmResponse.getContent(), t4 - t3);
                trace.add(llmStep);
                stepCallback.accept(llmStep);

                // 提取 SQL
                candidateSql = PromptBuilder.extractSqlFromCodeBlock(llmResponse.getContent());
                if (candidateSql.isBlank()) {
                    String error = "Failed to extract SQL from LLM response";
                    TraceStep extractErrStep = TraceStep.error("extract", error, null);
                    trace.add(extractErrStep);
                    stepCallback.accept(extractErrStep);
                    TraceWriter.writeTrace(domain, nlq, trace, reqId);
                    return ChatBIResult.fail(error, trace, reqId);
                }
                TraceStep extractStep = TraceStep.extract(candidateSql);
                trace.add(extractStep);
                stepCallback.accept(extractStep);

                // Step 4: 静态校验
                validationResult = validateSql(domain, candidateSql, retrievalResult, enableExplain);
                JSONObject issues = new JSONObject();
                issues.put("issues", validationResult.issues());
                Exception exp = null;
                if ((exp = validationResult.exception()) != null) {
                    Throwable cause = exp.getCause();
                    issues.put("exception", ExceptionUtils.getStackTrace(cause != null ? cause : exp));
                }
                TraceStep validateStep = TraceStep.validate(validationResult.valid(), validationResult.reason(), issues);
                trace.add(validateStep);
                stepCallback.accept(validateStep);

                if (validationResult.valid()) {
                    break; // 校验通过，退出循环
                }

                // 校验失败：检查是否为关键字白名单失败（不重试）
                if (validationResult.reason() != null && validationResult.reason().contains("keyword")) {
                    logger.warn("Keyword whitelist validation failed (no retry): {}", validationResult.reason());
                    TraceWriter.writeTrace(domain, nlq, trace, reqId);
                    return ChatBIResult.fail(validationResult.reasonAndIssue(), trace, reqId, validationResult.exception());
                }

                // 其它失败：重试
                if (attempt <= maxRetry) {
                    logger.info("Validation failed (attempt {}), retrying with error feedback", attempt);
                    userPrompt = PromptBuilder.buildRetryPrompt(nlq, retrievalResult.promptContext(),
                            candidateSql, validationResult.reasonAndIssue());
                } else {
                    logger.warn("Max retry reached, validation still failed");
                }
            }

            // Step 6: 执行（可选）
            QueryResult queryResult = null;
            boolean executeQuery = executionConfig == null || executionConfig.isExecuteQuery();
            if (Objects.requireNonNull(validationResult, "validationResult can not be null").valid() && executeQuery) {
                long t5 = System.currentTimeMillis();
                queryResult = executeQuery(domain, retrievalResult, candidateSql);
                long t6 = System.currentTimeMillis();
                TraceStep execStep = TraceStep.execute(queryResult.rowCount(), t6 - t5);
                trace.add(execStep);
                stepCallback.accept(execStep);
            }

            // 写入 trace
            TraceWriter.writeTrace(domain, nlq, trace, reqId);

            if (validationResult.valid()) {
                return ChatBIResult.success(candidateSql, queryResult, trace, reqId);
            } else {
                return ChatBIResult.fail(validationResult.reasonAndIssue(), trace, reqId, validationResult.exception());
            }

        } catch (Exception e) {
            logger.error("ChatBI ask failed", e);
            TraceStep exceptionStep = TraceStep.error("exception", e.getMessage(), e);
            trace.add(exceptionStep);
            stepCallback.accept(exceptionStep);
            TraceWriter.writeTrace(domain, nlq, trace, reqId);
            return ChatBIResult.fail("Internal error: " + e.getMessage(), trace, reqId);
        }
    }

    private LLMProvider.LLMResponse invokeLLM(String domain, UserPrompt userPrompt, List<String> systemPrompt) {
        if (this.llmProvider == null) {
            throw new IllegalStateException("llmProvider can not be null");
        }
        return this.llmProvider.chat(IAgentContext.createNull(), userPrompt, systemPrompt);
    }

    private ValidationResult validateSql(String domain, String sql, RetrievalResult context, boolean enableExplain) {
        // 第 0 步：关键字白名单（失败不重试）
        ValidationResult keywordResult = validationConfig != null ?
                keywordValidator.validate(sql, context, validationConfig) :
                keywordValidator.validate(sql, context);
        if (!keywordResult.valid()) {
            return keywordResult;
        }

        // 第 1 步：AST 校验
        if (validationConfig == null || validationConfig.isEnableAstCheck()) {
            ValidationResult astResult = astValidator.validate(sql, context);
            if (!astResult.valid()) {
                return astResult;
            }
        }

        // 第 2 步：EXPLAIN 校验（可选）
        if (enableExplain) {
            int explainTimeout = retryConfig != null ? retryConfig.getExplainTimeout() : 5;
            ExplainValidator explainValidator = new ExplainValidator(domain, explainTimeout);
            ValidationResult explainResult = explainValidator.validate(sql, context);
            if (!explainResult.valid()) {
                return explainResult;
            }
        }

        return ValidationResult.ok();
    }

    private QueryResult executeQuery(String domain, RetrievalResult retrievalResult, String sql) {
        DataSourceFactory dataSource = null;
        for (String objType : retrievalResult.objectTypes()) {
            OntologyObjectType objectType = Ontology.loadObjectTypeDetail(domain, objType);
            dataSource = objectType.getDataSourceBinding().getDataSource();
            break;
        }
        final QueryResult[] queryResult = new QueryResult[1];
        final int maxResultRows = executionConfig != null ? executionConfig.getMaxResultRows() : 200;
        Objects.requireNonNull(dataSource, "dataSource can not be null")
                .visitFirstConnection((conn) -> {
                    try {
                        //  通过以下方法创建QueryResult
                        List<Map<String, Object>> rows = Lists.newArrayList();
                        List<String> columns = Lists.newArrayList();
                        final boolean[] columnsInitialized = {false};
                        final int[] actualRowsRef = {0};
                        final boolean[] truncatedRef = {false};
                        conn.query(sql, result -> {
                            // 首次回调时初始化列名
                            if (!columnsInitialized[0]) {
                                java.sql.ResultSetMetaData meta = result.getMetaData();
                                int colCount = meta.getColumnCount();
                                for (int i = 1; i <= colCount; i++) {
                                    columns.add(meta.getColumnLabel(i));
                                }
                                columnsInitialized[0] = true;
                            }
                            actualRowsRef[0]++;
                            // 超过 maxResultRows 时标记截断并停止迭代
                            if (rows.size() >= maxResultRows) {
                                truncatedRef[0] = true;
                                return false;
                            }
                            // 构建当前行的 Map
                            Map<String, Object> row = new java.util.LinkedHashMap<>();
                            for (int i = 0; i < columns.size(); i++) {
                                row.put(columns.get(i), result.getObject(i + 1));
                            }
                            rows.add(row);
                            return true;
                        });
                        queryResult[0] = new QueryResult(columns, rows, rows.size(), truncatedRef[0], actualRowsRef[0]);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        return queryResult[0];
//        // 一期暂不实现，返回空结果
//        logger.warn("Query execution not implemented yet");
//
//        int queryTimeout = executionConfig != null ? executionConfig.getQueryTimeout() : 30;
//        // 实际执行时需要使用 Statement.setMaxRows(maxResultRows) 和 Statement.setQueryTimeout(queryTimeout)
//        return QueryResult.empty();
    }
}

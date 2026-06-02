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
import com.qlangtech.tis.plugin.ontology.chatbi.prompt.PromptBuilder;
import com.qlangtech.tis.plugin.ontology.chatbi.trace.TraceWriter;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.AstValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.ExplainValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.KeywordWhitelistValidator;
import com.qlangtech.tis.plugin.ontology.chatbi.validation.ValidationResult;
import com.qlangtech.tis.plugin.ontology.graphrag.DefaultGraphRAGService;
import com.qlangtech.tis.plugin.ontology.graphrag.GraphRAGService;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * ChatBI 服务默认实现（§5 T5：重试编排）。
 * <p>
 * 流水线：检索 → 拼装 Prompt → LLM 调用 → 静态校验 → EXPLAIN 校验（可选）→ 执行（可选）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class DefaultChatBIService implements ChatBIService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultChatBIService.class);

    private static final DefaultChatBIService INSTANCE = new DefaultChatBIService();

    private final GraphRAGService graphRAGService = DefaultGraphRAGService.getInstance();
    private final KeywordWhitelistValidator keywordValidator = new KeywordWhitelistValidator();
    private final AstValidator astValidator = new AstValidator();
    private final ExplainValidator explainValidator = new ExplainValidator();

    private static final int MAX_RETRY = 2;

    private LLMProvider llmProvider;

    public static DefaultChatBIService getInstance() {
        return INSTANCE;
    }

    private DefaultChatBIService() {
    }

    public void setLlmProvider(LLMProvider llmProvider) {
        this.llmProvider = Objects.requireNonNull(llmProvider, "param llmProvider can not be null");
    }

    @Override
    public ChatBIResult ask(String domain, String nlq, ChatBIOptions opts) {
        List<TraceStep> trace = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        try {
            // Step 1: 检索
            long t1 = System.currentTimeMillis();
            RetrievalResult retrievalResult = graphRAGService.retrieve(domain, nlq, opts.retrievalOptions());
            long t2 = System.currentTimeMillis();
            trace.add(TraceStep.retrieve(retrievalResult.objectTypes().size(), retrievalResult.linkers().size(), t2 - t1));

            if (retrievalResult.objectTypes().isEmpty()) {
                String error = "No relevant ontology found for query: " + nlq;
                trace.add(TraceStep.error("retrieve", error));
                TraceWriter.writeTrace(domain, nlq, trace);
                return ChatBIResult.fail(error, trace);
            }

            // Step 2: 拼装 Prompt
            List<String> systemPrompt = PromptBuilder.buildSystemPrompt();
            UserPrompt userPrompt = PromptBuilder.buildInitialPrompt(nlq, retrievalResult.promptContext());
            int tokens = PromptBuilder.estimateTokens(systemPrompt.get(0) + userPrompt.getPrompt());
            trace.add(TraceStep.prompt(tokens, systemPrompt.get(0), userPrompt.getPrompt()));

            // Step 3-5: LLM 调用 + 校验 + 重试循环
            String candidateSql = null;
            ValidationResult validationResult = null;
            int attempt = 0;

            while (attempt <= MAX_RETRY) {
                attempt++;

                // Step 3: LLM 调用
                long t3 = System.currentTimeMillis();
                LLMProvider.LLMResponse llmResponse = invokeLLM(domain, userPrompt, systemPrompt);
                long t4 = System.currentTimeMillis();

                if (!llmResponse.isSuccess()) {
                    String error = "LLM invocation failed: " + llmResponse.getErrorMessage();
                    trace.add(TraceStep.error("llm", error));
                    TraceWriter.writeTrace(domain, nlq, trace);
                    return ChatBIResult.fail(error, trace);
                }

                trace.add(TraceStep.llm(llmResponse.getModel(), llmResponse.getPromptTokens(),
                        llmResponse.getCompletionTokens(), llmResponse.getContent(), t4 - t3));

                // 提取 SQL
                candidateSql = PromptBuilder.extractSqlFromCodeBlock(llmResponse.getContent());
                if (candidateSql.isBlank()) {
                    String error = "Failed to extract SQL from LLM response";
                    trace.add(TraceStep.error("extract", error));
                    TraceWriter.writeTrace(domain, nlq, trace);
                    return ChatBIResult.fail(error, trace);
                }
                trace.add(TraceStep.extract(candidateSql));

                // Step 4: 静态校验
                validationResult = validateSql(candidateSql, retrievalResult, opts.enableExplain());
                JSONObject issues = new JSONObject();
                issues.put("issues", validationResult.issues());
                trace.add(TraceStep.validate(validationResult.valid(), validationResult.reason(), issues));

                if (validationResult.valid()) {
                    break; // 校验通过，退出循环
                }

                // 校验失败：检查是否为关键字白名单失败（不重试）
                if (validationResult.reason() != null && validationResult.reason().contains("keyword")) {
                    logger.warn("Keyword whitelist validation failed (no retry): {}", validationResult.reason());
                    TraceWriter.writeTrace(domain, nlq, trace);
                    return ChatBIResult.fail(validationResult.reason(), trace);
                }

                // 其它失败：重试
                if (attempt <= MAX_RETRY) {
                    logger.info("Validation failed (attempt {}), retrying with error feedback", attempt);
                    userPrompt = PromptBuilder.buildRetryPrompt(nlq, retrievalResult.promptContext(),
                            candidateSql, validationResult.reason());
                } else {
                    logger.warn("Max retry reached, validation still failed");
                }
            }

            // Step 6: 执行（可选）
            QueryResult queryResult = null;
            if (validationResult.valid() && opts.executeQuery()) {
                long t5 = System.currentTimeMillis();
                queryResult = executeQuery(domain, candidateSql);
                long t6 = System.currentTimeMillis();
                trace.add(TraceStep.execute(queryResult.rowCount(), t6 - t5));
            }

            // 写入 trace
            TraceWriter.writeTrace(domain, nlq, trace);

            if (validationResult.valid()) {
                return ChatBIResult.success(candidateSql, queryResult, trace);
            } else {
                List<String> is = validationResult.issues();
                return ChatBIResult.fail(validationResult.reason() //
                        + (CollectionUtils.isNotEmpty(is) ? (",issue:" + String.join("," + is)) : StringUtils.EMPTY), trace);
            }

        } catch (Exception e) {
            logger.error("ChatBI ask failed", e);
            trace.add(TraceStep.error("exception", e.getMessage()));
            TraceWriter.writeTrace(domain, nlq, trace);
            return ChatBIResult.fail("Internal error: " + e.getMessage(), trace);
        }
    }

    private LLMProvider.LLMResponse invokeLLM(String domain, UserPrompt userPrompt, List<String> systemPrompt) {
        // TODO: 从 EnableChatBI 配置中加载 LLM Provider
        // 暂时抛异常，提示未配置
        // throw new UnsupportedOperationException("LLM Provider not configured. Please enable ChatBI for domain: " + domain);
        if (this.llmProvider == null) {
            throw new IllegalStateException("llmProvider can not be null");
        }
        return this.llmProvider.chat(IAgentContext.createNull(), userPrompt, systemPrompt);
    }

    private ValidationResult validateSql(String sql, RetrievalResult context, boolean enableExplain) {
        // 第 0 步：关键字白名单（失败不重试）
        ValidationResult keywordResult = keywordValidator.validate(sql, context);
        if (!keywordResult.valid()) {
            return keywordResult;
        }

        // 第 1 步：AST 校验
        ValidationResult astResult = astValidator.validate(sql, context);
        if (!astResult.valid()) {
            return astResult;
        }

        // 第 2 步：EXPLAIN 校验（可选）
        if (enableExplain) {
            ValidationResult explainResult = explainValidator.validate(sql, context);
            if (!explainResult.valid()) {
                return explainResult;
            }
        }

        return ValidationResult.ok();
    }

    private QueryResult executeQuery(String domain, String sql) {
        // TODO: 从 DataSourceFactory 获取连接执行 SQL
        // 一期暂不实现，返回空结果
        logger.warn("Query execution not implemented yet");
        return QueryResult.empty();
    }
}

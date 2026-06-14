package com.qlangtech.tis.plugin.ontology.chatbi;

import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ontology.chatbi.config.ExecutionConfig;
import com.qlangtech.tis.plugin.ontology.chatbi.config.RetrievalConfig;
import com.qlangtech.tis.plugin.ontology.chatbi.config.RetryConfig;
import com.qlangtech.tis.plugin.ontology.chatbi.config.ValidationConfig;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class DefaultChatBIServiceITTest {

    @Test
    public void testAsk() throws Exception {

        FalconChatBITestExample chatBITestExample = FalconChatBITestExample.load();

        List<FalconTestCase> cases = chatBITestExample.getTestCasesByDbId("14", Optional.of(294), true);

        DefaultChatBIService chatBIService = new DefaultChatBIService();
        chatBIService.setLlmProvider(
                LLMProvider.load(IPluginContext.namedContext("test").setLoginUser((() -> "admin")), "qwen1"));

        // 配置 RetrievalConfig
        RetrievalConfig retrievalConfig = new RetrievalConfig();
        retrievalConfig.topKSeeds = 5;
        retrievalConfig.maxHops = 2;
        retrievalConfig.tokenBudget = 3000;
        retrievalConfig.includeValueExamples = false;

        // 配置 RetryConfig
        RetryConfig retryConfig = new RetryConfig();
        retryConfig.maxRetry = 2;
        retryConfig.explainTimeout = Duration.ofSeconds(5);

        // 配置 ValidationConfig
        ValidationConfig validationConfig = new ValidationConfig();
        validationConfig.enableExplain = true;
        validationConfig.enableKeywordCheck = true;
        validationConfig.enableAstCheck = true;
        validationConfig.allowedFirstKeywords = ValidationConfig.dftAllowedFirstKeywords();
        validationConfig.forbiddenKeywords = ValidationConfig.dftForbiddenKeywords();
        validationConfig.safeFunctions = ValidationConfig.dftSafeFunctions();

        // 配置 ExecutionConfig
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.executeQuery = true;
        executionConfig.maxResultRows = 200;
        executionConfig.queryTimeout = Duration.ofSeconds(30);

        chatBIService.setConfigs(retryConfig, validationConfig, executionConfig, retrievalConfig);

        int index = 1;
        ChatBIEvaluationResult evaluationResult = new ChatBIEvaluationResult();
        try {
            for (FalconTestCase c : cases) {
                evaluationResult.appendLine(index + ".nl:qid:" + c.getQuestionId() + ":" + c.getQuestion() + "------------------------------------------------");

                evaluationResult.appendLine(index + ".expect sql------------------------------------------------");
                evaluationResult.appendLine(c.getFirstSql());

                /**
                 * 执行查询
                 */
                ChatBIResult chatBIResult = chatBIService.ask("falcon_14", c.getQuestion());
                Assert.assertNotNull(chatBIResult);
                if (!chatBIResult.isSuccess()) {
                    throw new IllegalStateException(chatBIResult.error(), chatBIResult.exception());
                }

                evaluationResult.appendLine(index + ".generate------------------------------------------------");
                evaluationResult.appendLine(chatBIResult.sql());
                Map<String, List<String>> answer = c.getFirstAnswer();

                if (answer != null && !answer.isEmpty()) {
                    evaluationResult.appendLine(index + ".expected answer------------------------------------------------");
                    evaluationResult.appendTable(answer);
                }

                evaluationResult.appendLine(index + ".------------------------------------------------");
                evaluationResult.appendLine(StringUtils.EMPTY);
                index++;
            }
        } finally {
            evaluationResult.writeToFile();
        }


    }

    private static class ChatBIEvaluationResult {
        // private final StringBuilder buffer = new StringBuilder();
        private final PrintStream out;

        public ChatBIEvaluationResult() {
            try {
                this.out = new PrintStream(FileUtils.openOutputStream(new File("evaluate_result.txt"), false), true, TisUTF8.get());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        private ChatBIEvaluationResult appendLine(String content) {
            System.out.println(content);
            out.println(content);//.append("\n");
            return this;
        }

        /**
         * 将查询结果格式化为表格输出
         */
        private ChatBIEvaluationResult appendTable(Map<String, List<String>> data) {
            if (data == null || data.isEmpty()) {
                return appendLine("(empty result)");
            }

            List<String> columns = new ArrayList<>(data.keySet());
            if (columns.isEmpty()) {
                return appendLine("(no columns)");
            }

            // 获取行数
            int rowCount = data.values().stream().mapToInt(List::size).max().orElse(0);
            if (rowCount == 0) {
                return appendLine("(no rows)");
            }

            // 计算每列的最大宽度（考虑中文字符占用2个字符宽度）
            int[] columnWidths = new int[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                columnWidths[i] = displayWidth(column);

                List<String> values = data.get(column);
                if (values != null) {
                    for (String value : values) {
                        if (value != null) {
                            columnWidths[i] = Math.max(columnWidths[i], displayWidth(value));
                        }
                    }
                }

                // 最小宽度为4，最大宽度为50
                columnWidths[i] = Math.max(4, Math.min(50, columnWidths[i]));
            }

            // 构建分隔线
            StringBuilder separator = new StringBuilder("+");
            for (int width : columnWidths) {
                separator.append("-".repeat(width + 2)).append("+");
            }
            String separatorLine = separator.toString();

            // 打印表头
            appendLine(separatorLine);
            StringBuilder headerLine = new StringBuilder("|");
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                headerLine.append(" ").append(padRight(column, columnWidths[i])).append(" |");
            }
            appendLine(headerLine.toString());
            appendLine(separatorLine);

            // 打印数据行
            for (int row = 0; row < rowCount; row++) {
                StringBuilder rowLine = new StringBuilder("|");
                for (int i = 0; i < columns.size(); i++) {
                    List<String> values = data.get(columns.get(i));
                    String value = (values != null && row < values.size() && values.get(row) != null)
                            ? values.get(row) : "";

                    // 如果值过长，截断并添加省略号
                    if (displayWidth(value) > columnWidths[i]) {
                        value = truncate(value, columnWidths[i] - 3) + "...";
                    }

                    rowLine.append(" ").append(padRight(value, columnWidths[i])).append(" |");
                }
                appendLine(rowLine.toString());
            }
            appendLine(separatorLine);
            appendLine("Total: " + rowCount + " rows");

            return this;
        }

        /**
         * 计算字符串的显示宽度（中文字符算2个宽度）
         */
        private int displayWidth(String str) {
            if (str == null) {
                return 0;
            }
            int width = 0;
            for (char c : str.toCharArray()) {
                width += (c >= 0x4E00 && c <= 0x9FA5) ? 2 : 1; // 中文字符范围
            }
            return width;
        }

        /**
         * 右侧填充空格，使字符串达到指定的显示宽度
         */
        private String padRight(String str, int targetWidth) {
            if (str == null) {
                str = "";
            }
            int currentWidth = displayWidth(str);
            if (currentWidth >= targetWidth) {
                return str;
            }
            return str + " ".repeat(targetWidth - currentWidth);
        }

        /**
         * 截断字符串到指定的显示宽度
         */
        private String truncate(String str, int maxWidth) {
            if (str == null || displayWidth(str) <= maxWidth) {
                return str;
            }
            StringBuilder result = new StringBuilder();
            int width = 0;
            for (char c : str.toCharArray()) {
                int charWidth = (c >= 0x4E00 && c <= 0x9FA5) ? 2 : 1;
                if (width + charWidth > maxWidth) {
                    break;
                }
                result.append(c);
                width += charWidth;
            }
            return result.toString();
        }

        public void writeToFile() {
            out.flush();
        }
    }

}

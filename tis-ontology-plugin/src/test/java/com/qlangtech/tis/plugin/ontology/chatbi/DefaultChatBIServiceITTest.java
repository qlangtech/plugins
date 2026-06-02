package com.qlangtech.tis.plugin.ontology.chatbi;

import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class DefaultChatBIServiceITTest {

    @Test
    public void testAsk() throws Exception {

        FalconChatBITestExample chatBITestExample = FalconChatBITestExample.load();

        List<FalconTestCase> cases = chatBITestExample.getTestCasesByDbId("14");

        DefaultChatBIService chatBIService = DefaultChatBIService.getInstance();
        DefaultChatBIService.getInstance().setLlmProvider( //
                LLMProvider.load(IPluginContext.namedContext("test").setLoginUser((() -> "admin")), "qwen1"));
        int index = 1;
        ChatBIEvaluationResult evaluationResult = new ChatBIEvaluationResult();
        for (FalconTestCase c : cases) {
            evaluationResult.appendLine(index + ".nl:" + c.getQuestion() + "------------------------------------------------");

            evaluationResult.appendLine(index + ".expect sql------------------------------------------------");
            evaluationResult.appendLine(c.getFirstSql());

            ChatBIResult chatBIResult = chatBIService.ask("falcon_14"
                    , c.getQuestion(), ChatBIOptions.defaults());
            Assert.assertNotNull(chatBIResult);
            if (!chatBIResult.isSuccess()) {
                throw new IllegalStateException(chatBIResult.error());
            }

            evaluationResult.appendLine(index + ".generate------------------------------------------------");
            evaluationResult.appendLine(chatBIResult.sql());

            evaluationResult.appendLine(index + ".------------------------------------------------");
            evaluationResult.appendLine(StringUtils.EMPTY);
            index++;
        }

        evaluationResult.writeToFile();
    }

    private static class ChatBIEvaluationResult {
        private final StringBuilder buffer = new StringBuilder();

        public ChatBIEvaluationResult() {
        }

        private ChatBIEvaluationResult appendLine(String content) {
            System.out.println(content);
            buffer.append(content).append("\n");
            return this;
        }

        public void writeToFile() {
            try {
                FileUtils.write(new File("evaluate_result.txt"), this.buffer, TisUTF8.get());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
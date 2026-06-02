package com.qlangtech.tis.plugin.ontology.chatbi;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;
import java.util.Map;

/**
 * Falcon 测试案例数据结构
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/06/02
 */
public class FalconTestCase {

    @JSONField(name = "question_id")
    private int questionId;

    @JSONField(name = "db_id")
    private String dbId;

    @JSONField(name = "question")
    private String question;

    @JSONField(name = "SQL")
    private List<String> sql;

    @JSONField(name = "answer")
    private List<Map<String, List<String>>> answer;

    @JSONField(name = "is_order")
    private String isOrder;

    public int getQuestionId() {
        return questionId;
    }

    public void setQuestionId(int questionId) {
        this.questionId = questionId;
    }

    public String getDbId() {
        return dbId;
    }

    public void setDbId(String dbId) {
        this.dbId = dbId;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public List<String> getSql() {
        return sql;
    }

    public void setSql(List<String> sql) {
        this.sql = sql;
    }

    public List<Map<String, List<String>>> getAnswer() {
        return answer;
    }

    public void setAnswer(List<Map<String, List<String>>> answer) {
        this.answer = answer;
    }

    public String getIsOrder() {
        return isOrder;
    }

    public void setIsOrder(String isOrder) {
        this.isOrder = isOrder;
    }

    public String getFirstSql() {
        return sql != null && !sql.isEmpty() ? sql.get(0) : null;
    }

    public Map<String, List<String>> getFirstAnswer() {
        return answer != null && !answer.isEmpty() ? answer.get(0) : null;
    }

    public boolean isOrderSensitive() {
        return "1".equals(isOrder);
    }

    @Override
    public String toString() {
        return "FalconTestCase{" +
                "questionId=" + questionId +
                ", dbId='" + dbId + '\'' +
                ", question='" + question + '\'' +
                ", sqlCount=" + (sql != null ? sql.size() : 0) +
                ", isOrder='" + isOrder + '\'' +
                '}';
    }
}

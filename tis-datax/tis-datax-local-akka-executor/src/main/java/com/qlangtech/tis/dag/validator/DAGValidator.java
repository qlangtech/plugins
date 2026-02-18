package com.qlangtech.tis.dag.validator;

import com.qlangtech.tis.powerjob.algorithm.WorkflowDAGUtils;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * DAG 定义验证工具类
 * 集成 WorkflowDAGUtils.valid() 方法，提供友好的验证接口
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class DAGValidator {

    private static final Logger logger = LoggerFactory.getLogger(DAGValidator.class);

    /**
     * 验证结果
     */
    public static class ValidationResult {
        private boolean valid;
        private List<String> errors;

        public ValidationResult() {
            this.valid = true;
            this.errors = new ArrayList<>();
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public List<String> getErrors() {
            return errors;
        }

        public void addError(String error) {
            this.valid = false;
            this.errors.add(error);
        }
    }

    /**
     * 验证 DAG 定义
     *
     * @param dag DAG 定义
     * @return 验证结果
     */
    public static ValidationResult validate(PEWorkflowDAG dag) {
        ValidationResult result = new ValidationResult();

        // 基本检查
        if (dag == null) {
            result.addError("DAG cannot be null");
            return result;
        }

        if (dag.getNodes() == null || dag.getNodes().isEmpty()) {
            result.addError("DAG must contain at least one node");
            return result;
        }

        // 节点检查
        for (PEWorkflowDAG.Node node : dag.getNodes()) {
            if (node.getNodeId() == null) {
                result.addError("Node ID cannot be null");
            }
            if (node.getNodeName() == null || node.getNodeName().trim().isEmpty()) {
                result.addError("Node name cannot be empty for node: " + node.getNodeId());
            }
            if (node.getNodeType() == null) {
                result.addError("Node type cannot be null for node: " + node.getNodeId());
            }
        }

        // 边检查
        if (dag.getEdges() != null) {
            for (PEWorkflowDAG.Edge edge : dag.getEdges()) {
                if (edge.getFrom() == null || edge.getTo() == null) {
                    result.addError("Edge from/to cannot be null");
                }
            }
        }

        // 如果基本检查失败，直接返回
        if (!result.isValid()) {
            return result;
        }

        // 使用 WorkflowDAGUtils 进行深度验证
        try {
            boolean valid = WorkflowDAGUtils.valid(dag);
            if (!valid) {
                result.addError("DAG validation failed: contains cycle or isolated nodes");
            }
        } catch (Exception e) {
            logger.error("DAG validation error", e);
            result.addError("DAG validation error: " + e.getMessage());
        }

        return result;
    }

    /**
     * 验证 DAG 定义（简化版本）
     *
     * @param dag DAG 定义
     * @return 是否合法
     */
    public static boolean isValid(PEWorkflowDAG dag) {
        return validate(dag).isValid();
    }
}

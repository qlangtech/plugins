package com.qlangtech.tis.dag.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * TIS DAG 工作流目录初始化器
 * 在系统启动时创建必要的目录结构
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class WorkflowDirectoryInitializer {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowDirectoryInitializer.class);

    /**
     * 工作流基础目录名称
     */
    private static final String WORKFLOW_DIR_NAME = "workflow";

    /**
     * 初始化工作流目录结构
     * 创建 ${TIS_HOME}/workflow/ 目录
     *
     * @param tisHome TIS_HOME 路径
     * @return 工作流目录
     */
    public static File initWorkflowDirectory(File tisHome) {
        if (tisHome == null || !tisHome.exists()) {
            throw new IllegalArgumentException("TIS_HOME directory does not exist: " + tisHome);
        }

        File workflowDir = new File(tisHome, WORKFLOW_DIR_NAME);

        if (!workflowDir.exists()) {
            boolean created = workflowDir.mkdirs();
            if (created) {
                logger.info("Created workflow directory: {}", workflowDir.getAbsolutePath());
            } else {
                throw new IllegalStateException("Failed to create workflow directory: " + workflowDir.getAbsolutePath());
            }
        } else {
            logger.info("Workflow directory already exists: {}", workflowDir.getAbsolutePath());
        }

        return workflowDir;
    }

    /**
     * 获取工作流目录
     *
     * @param tisHome TIS_HOME 路径
     * @return 工作流目录
     */
    public static File getWorkflowDirectory(File tisHome) {
        return new File(tisHome, WORKFLOW_DIR_NAME);
    }
}

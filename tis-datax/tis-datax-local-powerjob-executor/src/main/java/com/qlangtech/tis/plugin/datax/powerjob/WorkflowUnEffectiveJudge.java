package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import tech.powerjob.common.model.PEWorkflowDAG;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/23
 */
public class WorkflowUnEffectiveJudge {
    private boolean unEffective;

    private final List<PEWorkflowDAG.Node> deletedWfNodes = Lists.newArrayList();
    private final Map<String /**tableName*/, PEWorkflowDAG.Node> existWfNodes = Maps.newHashMap();
    private PEWorkflowDAG.Node startInitNode;

    public WorkflowUnEffectiveJudge() {
        this(false);
    }

    public void addDeletedWfNode(PEWorkflowDAG.Node node) {
        deletedWfNodes.add(node);
        this.setUnEffective();
    }

    public List<PEWorkflowDAG.Node> getDeletedWfNodes() {
        return deletedWfNodes;
    }

    /**
     * 添加已经存在的workflow节点
     *
     * @param tabTriggers
     * @param node
     */
    public void addExistWfNode(SelectedTabTriggers tabTriggers, PEWorkflowDAG.Node node) {
        String tableName = tabTriggers.getTabName();
        existWfNodes.put(tableName, node);
        //   this.setUnEffective();
    }

    public Optional<PEWorkflowDAG.Node> getExistWfNode(String tableName) {
        return Optional.ofNullable(existWfNodes.get(tableName));
        //return existWfNodes.entrySet();
    }

    public WorkflowUnEffectiveJudge setUnEffective() {
        this.unEffective = true;
        return this;
    }

    public WorkflowUnEffectiveJudge(boolean unEffective) {
        this.unEffective = unEffective;
    }

    public boolean isUnEffective() {
        return this.unEffective;
    }

    public void setStatInitNode(PEWorkflowDAG.Node node) {
        this.startInitNode = node;
    }

    public Optional<PEWorkflowDAG.Node> getStartInitNode() {
        return Optional.ofNullable(startInitNode);
    }
}

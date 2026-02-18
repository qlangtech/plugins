package com.qlangtech.tis.dag.serialization;

import com.qlangtech.tis.dag.actor.message.CancelWorkflow;
import com.qlangtech.tis.dag.actor.message.DispatchTask;
import com.qlangtech.tis.dag.actor.message.NodeCompleted;
import com.qlangtech.tis.dag.actor.message.NodeTimeout;
import com.qlangtech.tis.dag.actor.message.QueryRunningQueue;
import com.qlangtech.tis.dag.actor.message.QueryWaitingQueue;
import com.qlangtech.tis.dag.actor.message.QueryWorkflowStatus;
import com.qlangtech.tis.dag.actor.message.StartWorkflow;
import com.qlangtech.tis.dag.actor.message.TaskExecutionMessage;
import com.qlangtech.tis.dag.actor.message.UpdateContext;
import com.qlangtech.tis.datax.NodeStatus;
import com.qlangtech.tis.datax.WorkflowRuntimeStatus;
import com.qlangtech.tis.powerjob.model.InstanceStatus;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.qlangtech.tis.powerjob.model.WorkflowNodeType;
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer;
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo;

/**
 * TIS DAG Kryo 序列化配置
 * 注册所有需要序列化的消息类和 DAG 相关类
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class TISKryoInitializer extends DefaultKryoInitializer {

    @Override
    public void postInit(ScalaKryo kryo) {
        super.postInit(kryo);

        // Register common Java classes
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.LinkedList.class);
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.TreeMap.class);
        kryo.register(java.util.TreeSet.class);
        kryo.register(java.util.Date.class);
        kryo.register(java.sql.Timestamp.class);

        // Register arrays
        kryo.register(String[].class);
        kryo.register(Object[].class);
        kryo.register(int[].class);
        kryo.register(long[].class);

        // Register DAGSchedulerActor messages
        kryo.register(StartWorkflow.class);
        kryo.register(NodeCompleted.class);
        kryo.register(NodeTimeout.class);
        kryo.register(UpdateContext.class);
        kryo.register(CancelWorkflow.class);

        // Register NodeDispatcherActor messages
        kryo.register(DispatchTask.class);
        kryo.register(TaskExecutionMessage.class);

        // Register DAGMonitorActor messages
        kryo.register(QueryWorkflowStatus.class);
        kryo.register(QueryWaitingQueue.class);
        kryo.register(QueryRunningQueue.class);

        // Register response messages
        kryo.register(WorkflowRuntimeStatus.class);
        kryo.register(NodeStatus.class);

        // Register PowerJob DAG classes
        kryo.register(PEWorkflowDAG.class);
        kryo.register(PEWorkflowDAG.Node.class);
        kryo.register(PEWorkflowDAG.Edge.class);

        // Register enums
        kryo.register(InstanceStatus.class);
        kryo.register(WorkflowNodeType.class);
    }
}

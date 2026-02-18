package com.qlangtech.tis.dag.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.dag.actor.message.DispatchTask;
import com.qlangtech.tis.dag.actor.message.NodeCompleted;
import com.qlangtech.tis.dag.actor.message.StartWorkflow;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.powerjob.model.InstanceStatus;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.qlangtech.tis.workflow.pojo.WorkflowDAGFileManager;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/11
 * @see WorkflowInstanceActor
 */
public class TestWorkflowInstanceActor implements TISEasyMock {

    static final String testDataXName = "mysql_mysql";

    @Before
    public void setUp() {
        ITISCoordinator.disableRemoteServer();
        CenterResource.setNotFetchFromCenterRepository();
        HttpUtils.mockConnMaker = null;
    }

    /**
     * DAG contains 10 nodes: 8 TASK + 2 CONTROL
     * Topology (two groups):
     *   paydetail:    3→4, 3→5, 4→2, 5→2, 2→1(CONTROL)
     *   orderdetail:  8→9, 8→10, 9→7, 10→7, 7→6(CONTROL)
     *
     * pipelineParallelism=1 (DEFAULT_PARALLELISM_IN_VM), tasks dispatched one at a time.
     * CONTROL nodes are completed synchronously in dispatchReadyNodes.
     * After all 8 TASK nodes complete, completeWorkflow() calls stop(self).
     */
    @Test
    public void testDAGCompleteWorkflow() {
        ActorSystem system = ActorSystem.create("test",
                ConfigFactory.parseString("akka.actor.provider=local")
                        .withFallback(ConfigFactory.load()));
        try {
            new TestKit(system) {{
                int taskId = 9999;

                IWorkFlowBuildHistoryDAO dao = mock("dao", IWorkFlowBuildHistoryDAO.class);

                WorkFlowBuildHistory history = new WorkFlowBuildHistory();
                IDataxProcessor dataxProcessor = DataxProcessor.load(null, testDataXName);
                File dagRuntime = new File(dataxProcessor.getDataXWorkDir(null),
                        WorkflowDAGFileManager.DAG_SPEC_FILENAME);
                history.setDagRuntime(dagRuntime.getAbsolutePath());

                EasyMock.expect(dao.loadFromWriteDB(taskId)).andReturn(history);
                EasyMock.expect(dao.updateByPrimaryKeySelective(
                        EasyMock.anyObject(WorkFlowBuildHistory.class))).andReturn(1).anyTimes();

                // mock HTTP for TaskSoapUtils.createTaskComplete
                HttpUtils.addMockApply(0, "do_task_complete",
                        "do_task_complete_response.json", TestWorkflowInstanceActor.class);

                // nodeDispatcher probe intercepts DispatchTask messages
                TestKit dispatcherProbe = new TestKit(system);

                replay();

                ActorRef actor = system.actorOf(
                        WorkflowInstanceActor.props(dispatcherProbe.getRef(), dao));

                watch(actor);

                DataXName dataXName = DataXName.createDataXPipeline(testDataXName);
                actor.tell(new StartWorkflow(taskId, dataXName), getRef());

                // pipelineParallelism=1, tasks come one at a time
                // 8 TASK nodes total, CONTROL nodes handled synchronously
                int dispatched = 0;
                while (dispatched < 8) {
                    DispatchTask task = dispatcherProbe.expectMsgClass(
                            Duration.ofMinutes(5), DispatchTask.class);
                    actor.tell(new NodeCompleted(taskId,
                                    task.getNode().getNodeId(), InstanceStatus.SUCCEED),
                            dispatcherProbe.getRef());
                    dispatched++;
                }

                // completeWorkflow() calls getContext().stop(getSelf())
                expectTerminated(Duration.ofMinutes(5), actor);

                verifyAll();
            }};
        } finally {
            TestKit.shutdownActorSystem(system);
        }
    }
}

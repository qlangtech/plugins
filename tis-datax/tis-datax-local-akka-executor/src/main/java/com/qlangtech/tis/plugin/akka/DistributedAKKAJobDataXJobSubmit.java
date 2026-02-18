package com.qlangtech.tis.plugin.akka;

import akka.pattern.Patterns;
import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.dag.TISActorSystem;
import com.qlangtech.tis.dag.actor.WorkflowInstanceActor;
import com.qlangtech.tis.dag.actor.message.QueryActiveWorkers;
import com.qlangtech.tis.dag.actor.message.QueryClusterStatus;
import com.qlangtech.tis.dag.actor.message.QueryWorkflowStatus;
import com.qlangtech.tis.datax.ActorSystemStatus;
import com.qlangtech.tis.datax.DataXJobSubmitAkkaClusterSupport;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.WorkflowRuntimeStatus;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.BasicWorkflowPayload;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.qlangtech.tis.workflow.pojo.WorkflowDAGFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * DistributedPowerJobDataXJobSubmit
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/31
 */
@TISExtension()
@Public
public class DistributedAKKAJobDataXJobSubmit extends BasicDistributedSPIDataXJobSubmit<AkkaWorkflow> implements DataXJobSubmitAkkaClusterSupport {
    private static final Logger logger = LoggerFactory.getLogger(DistributedAKKAJobDataXJobSubmit.class);

    @Override
    protected BasicWorkflowPayload<AkkaWorkflow> createWorkflowPayload(
            IControlMsgHandler module, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory, SqlTaskNodeMeta.SqlDataFlowTopology topology) {
        DataxProcessor dataxProcessor = null;
        return new AkkaWorkflowPayload(dataxProcessor, this);
    }

    @Override
    protected BasicWorkflowPayload<AkkaWorkflow> createApplicationPayload(IExecChainContext execChainContext,
                                                                          DataXName appName) {
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, appName);

        return new AkkaPipelinePayload(getStatusRpc(), dataxProcessor, TISActorSystem.get().getDaoFacade(), this);
    }


    @Override
    public InstanceType getType() {
        return InstanceType.AKKA;
    }

    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ActorSystemStatus queryActorSystemStatus() {
        TISActorSystem akkaSys = TISActorSystem.get();
        ActorSystemStatus status = akkaSys.collectStatus();

        try {
            Duration timeout = Duration.create(10, TimeUnit.SECONDS);
            long timeoutMillis = timeout.toMillis();

            // Query cluster members from ClusterManagerActor
            Future<Object> clusterFuture = Patterns.ask(
                    akkaSys.getClusterManagerActor(), new QueryClusterStatus(), timeoutMillis);
            Object clusterResult = Await.result(clusterFuture, timeout);
            if (clusterResult instanceof List) {
                status.setClusterMembers((List<ActorSystemStatus.ClusterMemberInfo>) clusterResult);
            }

            // Query active workers from NodeDispatcherActor
            Future<Object> workersFuture = Patterns.ask(
                    akkaSys.getNodeDispatcherActor(), new QueryActiveWorkers(), timeoutMillis);
            Object workersResult = Await.result(workersFuture, timeout);
            if (workersResult instanceof List) {
                status.setActiveWorkers((List<ActorSystemStatus.ActiveWorkerInfo>) workersResult);
            }
        } catch (Exception e) {
            logger.warn("failed to query actor system detail status", e);
        }

        return status;
    }

    /**
     * @param taskId DataXName dataXName,
     * @return
     * @see WorkflowInstanceActor#handleQueryWorkflowStatus(QueryWorkflowStatus)
     */
    @Override
    public WorkflowRuntimeStatus doQueryWorkflowStatus(DataXName dataXName, Integer taskId) {
        TISActorSystem akkaSys = TISActorSystem.get();

        // 通过 Akka Cluster Sharding 检查 Actor 是否活跃，避免自动创建空 Actor
        if (!akkaSys.isWorkflowInstanceActive(taskId)) {
            return loadHistoryWorkflowRuntimeStatus(dataXName, taskId);
        }

        QueryWorkflowStatus queryMsg = new QueryWorkflowStatus(taskId);
        try {
            Future<Object> future = Patterns.ask(
                    akkaSys.getWorkflowInstanceRegion(), queryMsg, Duration.create(30, TimeUnit.SECONDS).toMillis());
            Object result = Await.result(future, Duration.create(30, TimeUnit.SECONDS));
            if (result instanceof WorkflowRuntimeStatus) {
                return (WorkflowRuntimeStatus) result;
            }
            throw new IllegalStateException("unexpected response type: " + result.getClass().getName());
        } catch (TisException e) {
            // Actor was auto-created by Cluster Sharding (race condition), it will self-stop
            logger.warn("workflow instance actor not initialized, taskId: {}", taskId);
            return loadHistoryWorkflowRuntimeStatus(dataXName, taskId);
        } catch (Exception e) {
            throw new RuntimeException("failed to query workflow status, taskId:" + taskId, e);
        }
    }

    private static WorkflowRuntimeStatus loadHistoryWorkflowRuntimeStatus(DataXName dataXName, Integer taskId) {
        try {
            WorkflowRuntimeStatus wfStatus = new WorkflowRuntimeStatus();
            wfStatus.setInstanceId(taskId);
            PEWorkflowDAG workflowDAG = WorkflowDAGFileManager.loadDAG(dataXName, taskId);
            wfStatus.appendDAG(workflowDAG);
            return wfStatus;
        } catch (FileNotFoundException e) {
            // throw new RuntimeException(e);
            logger.warn(e.getMessage(), e);
            return null;
        }

    }


    /**
     * 初始化 TIS Actor System
     * <p>
     * 实现步骤：
     * 1. 获取 Spring ApplicationContext
     * 2. 从 Spring 容器中获取 DAO 依赖
     * 3. 创建 TISActorSystem 实例
     * 4. 初始化 Actor System
     * 5. 将实例保存到 ServletContext（供其他组件使用）
     *
     *
     */
    @Override
    public void launchAkkaCluster() {
        logger.info("Initializing TIS Actor System...");

        try {
            // 1. 获取 Spring ApplicationContext
//            WebApplicationContext applicationContext =
//                    WebApplicationContextUtils.getWebApplicationContext(sce.getServletContext());
//
//            if (applicationContext == null) {
//                logger.warn("Spring ApplicationContext not available, skipping Actor System initialization");
//                return;
//            }
            // IWorkflowDAOFacade workflowDAOFacade = WorkflowDAOFacadeRegister.getWorkflowDAOFacade();
            // 2. 从 Spring 容器中获取 DAO 依赖
//            IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO = workflowDAOFacade.getWorkFlowBuildHistoryDAO();// applicationContext.getBean(IWorkFlowBuildHistoryDAO.class);
//            IDAGNodeExecutionDAO dagNodeExecutionDAO = workflowDAOFacade.getDagNodeExecutionDAO();// applicationContext.getBean(IDAGNodeExecutionDAO.class);

//            if (workflowDAOFacade == null) {
//                throw new IllegalStateException("Required DAO beans not found, skipping Actor System initialization");
//            }
            DAORestDelegateFacade akkaClusterDependenceDao = DAORestDelegateFacade.createAKKAClusterDependenceDao();
            // 3. 创建 TISActorSystem 实例
            TISActorSystem tisActorSystem = TISActorSystem.createAndInit(akkaClusterDependenceDao);

            // 4. 初始化 Actor System
            tisActorSystem.initialize();

            // 5. 将实例保存到 ServletContext（供其他组件使用）
            // sce.getServletContext().setAttribute(ATTR_TIS_ACTOR_SYSTEM, tisActorSystem);
//      TISActorSystem.set(tisActorSystem);
            logger.info("TIS Actor System initialized successfully");

        } catch (Exception e) {
            logger.error("Failed to initialize TIS Actor System", e);
            throw new RuntimeException("Failed to initialize TIS Actor System", e);
        }
    }
}

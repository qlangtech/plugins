package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dag.BatchJobCrontab;
import com.qlangtech.tis.dag.actor.message.LoadSchedules;
import com.qlangtech.tis.dag.actor.message.QuerySchedulerDetail;
import com.qlangtech.tis.dag.actor.message.RegisterSchedule;
import com.qlangtech.tis.dag.actor.message.ScheduleTriggered;
import com.qlangtech.tis.dag.actor.message.UnregisterSchedule;
import com.qlangtech.tis.datax.DAGSchedulerDetail;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate.DataXProcessorTemplateManipulateStore;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Cron-based scheduler actor. Loads all scheduled workflows on startup,
 * creates Akka Scheduler timed tasks based on cron configuration,
 * and sends StartWorkflow to WorkflowInstanceActor sharding region on trigger.
 * <p>
 * Core responsibilities:
 * 1. On startup, load all BatchJobCrontab plugin instances and schedule cron tasks
 * 2. On cron trigger, send StartWorkflow to WorkflowInstanceActor sharding region
 * 3. Support dynamic register/unregister of schedules
 * 4. Auto-recover on TIS restart via preStart() -> LoadSchedules
 *
 * @author baisui
 */
public class DAGSchedulerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(DAGSchedulerActor.class);

    private final ActorRef workflowInstanceRegion;
    private final IWorkFlowBuildHistoryDAO buildHistoryDAO;

    private static class ActiveSchedules {
        private final Map<DataXName, ScheduleEntry> schedules = new HashMap<>();

        public ScheduleEntry get(DataXName dataXName) {
            return schedules.get(dataXName);
        }

        public ScheduleEntry remove(DataXName dataXName) {
            return schedules.remove(dataXName);
        }

        public void put(DataXName dataXName, ScheduleEntry entry) {
            this.schedules.put(dataXName, entry);
        }
    }

    private final ActiveSchedules activeSchedules = new ActiveSchedules();

    public static Props props(ActorRef workflowInstanceRegion, IWorkFlowBuildHistoryDAO buildHistoryDAO) {
        return Props.create(DAGSchedulerActor.class,
                () -> new DAGSchedulerActor(workflowInstanceRegion, buildHistoryDAO));
    }

    private DAGSchedulerActor(ActorRef workflowInstanceRegion, IWorkFlowBuildHistoryDAO buildHistoryDAO) {
        this.workflowInstanceRegion = Objects.requireNonNull(workflowInstanceRegion,
                "workflowInstanceRegion can not be null");
        this.buildHistoryDAO = Objects.requireNonNull(buildHistoryDAO,
                "buildHistoryDAO can not be null");
    }

    @Override
    public void preStart() {
        getSelf().tell(LoadSchedules.INSTANCE, getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LoadSchedules.class, this::handleLoadSchedules)
                .match(ScheduleTriggered.class, this::handleScheduleTriggered)
                .match(RegisterSchedule.class, this::handleRegisterSchedule)
                .match(UnregisterSchedule.class, this::handleUnregisterSchedule)
                .match(QuerySchedulerDetail.class, this::handleQuerySchedulerDetail)
                .build();
    }

    /**
     * Load all BatchJobCrontab plugin instances and register Akka scheduled tasks.
     * <p>
     * Uses DefaultDataXProcessorManipulate's in-memory registry to find pipelines
     * with crontab configured. Pipelines not yet loaded into memory will be
     * discovered and registered via RegisterSchedule messages from the console.
     */
    private void handleLoadSchedules(LoadSchedules msg) {
        logger.info("loading scheduled workflows from BatchJobCrontab plugin instances");
        try {
            Map<String, DataXProcessorTemplateManipulateStore> registry =
                    DefaultDataXProcessorManipulate.getManipulateRegistry();

            int loaded = 0;
            for (Map.Entry<String, DataXProcessorTemplateManipulateStore> entry : registry.entrySet()) {
                String pipelineName = entry.getKey();
                DataXProcessorTemplateManipulateStore store = entry.getValue();
                for (DefaultDataXProcessorManipulate manipulate : store.getManipulates()) {
                    if (BatchJobCrontab.KEY_CRONTAB.equals(manipulate.identityValue())) {
                        BatchJobCrontab crontab = (BatchJobCrontab) manipulate;
                        if (Boolean.TRUE.equals(crontab.turnOn) && crontab.crontab != null) {
                            registerCronSchedule(pipelineName, StoreResourceType.DataApp, crontab.crontab);
                            loaded++;
                        }
                    }
                }
            }
            logger.info("loaded {} scheduled workflows from plugin registry", loaded);
        } catch (Exception e) {
            logger.error("failed to load scheduled workflows from plugin registry", e);
        }
    }

    /**
     * Handle cron trigger: check if a workflow instance is already running for this pipeline;
     * if not, create a build history record and send StartWorkflow to the sharding region.
     *
     * @see com.qlangtech.tis.plugin.akka.AkkaPipelinePayload#submitToAkkaCluster
     */
    private void handleScheduleTriggered(ScheduleTriggered msg) {
        String pipelineName = msg.getPipelineName();
        StoreResourceType resType = msg.getResType();
        logger.info("schedule triggered for pipeline: {}", pipelineName);

        ScheduleEntry entry = activeSchedules.get(msg.getDataName());
        if (entry != null) {
            entry.lastTriggerTime = System.currentTimeMillis();
        }

        try {
            if (hasRunningInstance(msg.getDataName())) {
                logger.warn("pipeline {} has running instance, skipping this trigger", pipelineName);
                scheduleNextIfActive(msg.getDataName());
                return;
            }
            DataXJobSubmit dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit();
            long triggerTimestamp = System.currentTimeMillis();
            DataXPipelineExecContext execContext = new DataXPipelineExecContext(pipelineName, triggerTimestamp);
            execContext.setAttribute(TriggerType.class.getName(), TriggerType.CRONTAB);
//            RpcServiceReference rpcServiceRef = StatusRpcClientFactory.getService(ITISCoordinator.create());
//            IDataxProcessor processor = execContext.getProcessor();
//            DataXCfgGenerator.GenerateCfgs cfgFileNames
//                    = processor.getDataxCfgFileNames(null, Optional.empty());
//            Pair<DAGSessionSpec, List<Pair<ISelectedTab, SelectedTabTriggers>>> spec = DAGSessionSpec.createDAGSessionSpec(
//                    execContext, rpcServiceRef, processor, cfgFileNames, dataXJobSubmit);
            //DAGSessionSpec sessionSpec = spec.getLeft();

//            PEWorkflowDAG dag = Objects.requireNonNull(sessionSpec.getDAG() //
//                    , "pipeline:" + pipelineName + " relevant dag can not be null");

            // 4. 保存 DAG 定义到文件系统
//            WorkflowDAGFileManager fileManager //
//                    = new WorkflowDAGFileManager(processor.getDataXWorkDir(null), true);
//            File dagSpecPath = fileManager.saveDagSpec(pipelineName, dag);
//            CreateNewTaskResult newTask = IExecChainContext.createNewTask(execContext, TriggerType.CRONTAB, dagSpecPath);


            TriggerBuildResult triggerResult = dataXJobSubmit.triggerJob(execContext, msg.getDataName());

            //  WorkFlowBuildHistory buildHistory = new WorkFlowBuildHistory();
//            buildHistory.setAppName(pipelineName);
//            buildHistory.setStartTime(new Date());
            //  buildHistory.setInstanceStatus(InstanceStatus.WAITING.getDesc());
            // TriggerType
            //  buildHistory.setTriggerType(2); // 2 = cron triggered
            // buildHistory.setCreateTime(new Date());
            // buildHistory.setOpTime(new Date());
            // Integer instanceId = buildHistoryDAO.insertSelective(buildHistory);

            // DataXName dataXName = new DataXName(pipelineName, resType);
          //  StartWorkflow startMsg = new StartWorkflow(newTask.getTaskid(), msg.getDataName());
           // workflowInstanceRegion.tell(startMsg, getSelf());
            logger.info("sent StartWorkflow for pipeline: {}, instanceId: {}", pipelineName, triggerResult.getTaskid());
        } catch (Exception e) {
            logger.error("failed to handle schedule trigger for pipeline: " + pipelineName, e);
        }

        scheduleNextIfActive(msg.getDataName());
    }

    /**
     * Register a new cron schedule dynamically. If a schedule with the same pipelineName
     * already exists, cancel the old one first.
     */
    public void handleRegisterSchedule(RegisterSchedule msg) {
        try {
            logger.info("registering schedule for pipeline: {}, cron: {}", msg.getPipelineName(), msg.getCronExpression());
            registerCronSchedule(msg.getPipelineName(), msg.getResType(), msg.getCronExpression());
            getSender().tell(akka.Done.getInstance(), getSelf());
        } catch (Exception e) {
            logger.error("failed to handle register schedule for pipeline: " + msg.getPipelineName(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    /**
     * Remove a cron schedule dynamically, cancelling the pending Akka scheduled task.
     */
    public void handleUnregisterSchedule(UnregisterSchedule msg) {
        try {
            final DataXName dataXName = new DataXName(msg.getPipelineName(), msg.getResType());
            logger.info("unregistering schedule for pipeline: {}", dataXName);

            ScheduleEntry entry = activeSchedules.remove(dataXName);
            if (entry != null && entry.scheduledTask != null) {
                entry.scheduledTask.cancel();
                logger.info("cancelled schedule for pipeline: {}", dataXName);
            }
            BatchJobCrontab crontab = getBatchJobCrontab(dataXName);
            if (crontab != null) {
                crontab.turnOn = false;
            } else {
                // 这里应该是已经被物理删除了
            }

            getSender().tell(akka.Done.getInstance(), getSelf());
        } catch (Exception e) {
            logger.error("failed to handle unregister schedule for pipeline: " + msg.getPipelineName(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    private void registerCronSchedule(String pipelineName, StoreResourceType resType, String cronExpression) {
        // Cancel existing schedule if present
        final DataXName dataXName = new DataXName(pipelineName, resType);
        ScheduleEntry existing = activeSchedules.remove(dataXName);
        if (existing != null && existing.scheduledTask != null) {
            existing.scheduledTask.cancel();
        }
        BatchJobCrontab crontab = Objects.requireNonNull(getBatchJobCrontab(dataXName) //
                , "dataX:" + dataXName.getPipelineName() + " relevant crontab can not be null");
        crontab.crontab = cronExpression;
        crontab.turnOn = true;
        ScheduleEntry entry = new ScheduleEntry(pipelineName, resType, cronExpression);
        entry.registerTime = System.currentTimeMillis();
        try {
            scheduleNext(entry);
            activeSchedules.put(dataXName, entry);
            logger.info("registered schedule for pipeline: {}, cron: {}", pipelineName, cronExpression);
        } catch (Exception e) {
            logger.error("failed to register schedule for pipeline: " + pipelineName, e);
        }
    }

    private BatchJobCrontab getBatchJobCrontab(DataXName dataXName) {
        return DefaultDataXProcessorManipulate.getManipulateStore(dataXName, true) //
                .getManipuldate(IdentityName.create(BatchJobCrontab.KEY_CRONTAB), BatchJobCrontab.class);
    }

    /**
     * Calculate next fire time from cron expression and schedule an Akka scheduleOnce.
     */
    private void scheduleNext(ScheduleEntry entry) {
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
        ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(entry.cronExpression));
        ZonedDateTime nextFireTime = executionTime.nextExecution(ZonedDateTime.now())
                .orElseThrow(() -> new IllegalStateException("can not calculate next execution time for cron: " + entry.cronExpression));
        long delayMillis = nextFireTime.toInstant().toEpochMilli() - System.currentTimeMillis();
        if (delayMillis < 0) {
            delayMillis = 0;
        }

        ScheduleTriggered triggerMsg = new ScheduleTriggered(entry.pipelineName, entry.resType);
        Cancellable cancellable = getContext().getSystem().scheduler().scheduleOnce(
                Duration.create(delayMillis, TimeUnit.MILLISECONDS),
                getSelf(),
                triggerMsg,
                getContext().getDispatcher(),
                getSelf()
        );
        entry.scheduledTask = cancellable;
        logger.debug("scheduled next trigger for pipeline: {} at {}", entry.pipelineName, nextFireTime);
    }

    /**
     * Re-schedule next trigger only if the pipeline is still in active schedules.
     */
    private void scheduleNextIfActive(DataXName pipelineName) {
        ScheduleEntry entry = activeSchedules.get(pipelineName);
        if (entry != null) {
            try {
                scheduleNext(entry);
            } catch (Exception e) {
                logger.error("failed to schedule next trigger for pipeline: " + pipelineName, e);
            }
        }
    }

    /**
     * Check if there's already a RUNNING/WAITING/QUEUED instance for the given pipeline.
     */
    private boolean hasRunningInstance(DataXName pipelineName) {
//        WorkFlowBuildHistoryCriteria criteria = new WorkFlowBuildHistoryCriteria();
//        WorkFlowBuildHistoryCriteria.Criteria c = criteria.createCriteria();
//        c.andAppNameEqualTo(pipelineName);

        return buildHistoryDAO.hasRunningInstance(pipelineName);

//        List<WorkFlowBuildHistory> histories = buildHistoryDAO.selectByExample(criteria);
//        for (WorkFlowBuildHistory h : histories) {
//            InstanceStatus status = InstanceStatus.of(h.getInstanceStatus());
//            if (InstanceStatus.RUNNING == status
//                    || InstanceStatus.WAITING == status
//                    || InstanceStatus.QUEUED == status) {
//                return true;
//            }
//        }
//        return false;
    }

    /**
     * Handle query for all scheduled crontab entries (both active and inactive).
     * Iterates the manipulate registry to find all BatchJobCrontab instances,
     * supplements active schedule info (registerTime, lastTriggerTime, nextFireTime).
     */
    private void handleQuerySchedulerDetail(QuerySchedulerDetail msg) {
        DAGSchedulerDetail detail = new DAGSchedulerDetail();
        try {
            CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));
            Map<String, DataXProcessorTemplateManipulateStore> registry =
                    DefaultDataXProcessorManipulate.getManipulateRegistry();

            for (Map.Entry<String, DataXProcessorTemplateManipulateStore> registryEntry : registry.entrySet()) {
                DataXName pipelineName = DataXName.createDataXPipeline(registryEntry.getKey());
                DataXProcessorTemplateManipulateStore store = registryEntry.getValue();
                for (DefaultDataXProcessorManipulate manipulate : store.getManipulates()) {
                    if (BatchJobCrontab.KEY_CRONTAB.equals(manipulate.identityValue())) {
                        BatchJobCrontab crontab = (BatchJobCrontab) manipulate;
                        DAGSchedulerDetail.ScheduleEntryInfo info = new DAGSchedulerDetail.ScheduleEntryInfo();
                        info.setPipelineName(pipelineName.getPipelineName());
                        info.setCronExpression(crontab.crontab);
                        info.setTurnOn(Boolean.TRUE.equals(crontab.turnOn));

                        ScheduleEntry activeEntry = activeSchedules.get(pipelineName);
                        if (activeEntry != null) {
                            crontab.crontab = activeEntry.cronExpression;
                            info.setTurnOn(true);
                            info.setCronExpression(activeEntry.cronExpression);
                            info.setRegisterTime(activeEntry.registerTime);
                            info.setLastTriggerTime(activeEntry.lastTriggerTime);
                            // calculate next fire time
                            if (info.isTurnOn() && crontab.crontab != null) {
                                try {
                                    ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(crontab.crontab));
                                    long nextFire = executionTime.nextExecution(ZonedDateTime.now())
                                            .map(zdt -> zdt.toInstant().toEpochMilli())
                                            .orElse(-1L);
                                    info.setNextFireTime(nextFire);
                                } catch (Exception e) {
                                    logger.warn("failed to calculate next fire time for pipeline: {}", pipelineName, e);
                                    info.setNextFireTime(-1);
                                }
                            } else {
                                info.setNextFireTime(-1);
                            }
                        } else {
                            info.setRegisterTime(-1);
                            info.setLastTriggerTime(-1);
                            info.setNextFireTime(-1);
                        }

                        detail.getSchedules().add(info);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("failed to query scheduler detail", e);
        }
        getSender().tell(detail, getSelf());
    }

    /**
     * Internal record for an active schedule entry.
     */
    private static class ScheduleEntry {
        final String pipelineName;
        final StoreResourceType resType;
        final String cronExpression;
        Cancellable scheduledTask;
        long registerTime;
        long lastTriggerTime = -1;

        ScheduleEntry(String pipelineName, StoreResourceType resType, String cronExpression) {
            this.pipelineName = pipelineName;
            this.resType = resType;
            this.cronExpression = cronExpression;
        }
    }
}
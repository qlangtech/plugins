package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.qlangtech.tis.dag.BatchJobCrontab;
import com.qlangtech.tis.dag.actor.message.LoadSchedules;
import com.qlangtech.tis.dag.actor.message.RegisterSchedule;
import com.qlangtech.tis.dag.actor.message.ScheduleTriggered;
import com.qlangtech.tis.dag.actor.message.StartWorkflow;
import com.qlangtech.tis.dag.actor.message.UnregisterSchedule;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate.DataXProcessorTemplateManipulateStore;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.time.ZonedDateTime;
import java.util.Date;
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

    private final Map<String, ScheduleEntry> activeSchedules = new HashMap<>();

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
     */
    private void handleScheduleTriggered(ScheduleTriggered msg) {
        String pipelineName = msg.getPipelineName();
        StoreResourceType resType = msg.getResType();
        logger.info("schedule triggered for pipeline: {}", pipelineName);

        try {
            if (hasRunningInstance(pipelineName)) {
                logger.warn("pipeline {} has running instance, skipping this trigger", pipelineName);
                scheduleNextIfActive(pipelineName);
                return;
            }
            DataXJobSubmit dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit();
            long triggerTimestamp = System.currentTimeMillis();
            DataXPipelineExecContext execContext = new DataXPipelineExecContext(pipelineName, triggerTimestamp);


            dataXJobSubmit.triggerJob(execContext, msg.getDataName());

            WorkFlowBuildHistory buildHistory = new WorkFlowBuildHistory();
            buildHistory.setAppName(pipelineName);
            buildHistory.setStartTime(new Date());
          //  buildHistory.setInstanceStatus(InstanceStatus.WAITING.getDesc());
            // TriggerType
            buildHistory.setTriggerType(2); // 2 = cron triggered
            buildHistory.setCreateTime(new Date());
            buildHistory.setOpTime(new Date());
            Integer instanceId = buildHistoryDAO.insertSelective(buildHistory);

            DataXName dataXName = new DataXName(pipelineName, resType);
            StartWorkflow startMsg = new StartWorkflow(instanceId, dataXName);
            workflowInstanceRegion.tell(startMsg, getSelf());
            logger.info("sent StartWorkflow for pipeline: {}, instanceId: {}", pipelineName, instanceId);
        } catch (Exception e) {
            logger.error("failed to handle schedule trigger for pipeline: " + pipelineName, e);
        }

        scheduleNextIfActive(pipelineName);
    }

    /**
     * Register a new cron schedule dynamically. If a schedule with the same pipelineName
     * already exists, cancel the old one first.
     */
    private void handleRegisterSchedule(RegisterSchedule msg) {
        logger.info("registering schedule for pipeline: {}, cron: {}", msg.getPipelineName(), msg.getCronExpression());
        registerCronSchedule(msg.getPipelineName(), msg.getResType(), msg.getCronExpression());
    }

    /**
     * Remove a cron schedule dynamically, cancelling the pending Akka scheduled task.
     */
    private void handleUnregisterSchedule(UnregisterSchedule msg) {
        String pipelineName = msg.getPipelineName();
        logger.info("unregistering schedule for pipeline: {}", pipelineName);

        ScheduleEntry entry = activeSchedules.remove(pipelineName);
        if (entry != null && entry.scheduledTask != null) {
            entry.scheduledTask.cancel();
            logger.info("cancelled schedule for pipeline: {}", pipelineName);
        }
    }

    private void registerCronSchedule(String pipelineName, StoreResourceType resType, String cronExpression) {
        // Cancel existing schedule if present
        ScheduleEntry existing = activeSchedules.remove(pipelineName);
        if (existing != null && existing.scheduledTask != null) {
            existing.scheduledTask.cancel();
        }

        ScheduleEntry entry = new ScheduleEntry(pipelineName, resType, cronExpression);
        try {
            scheduleNext(entry);
            activeSchedules.put(pipelineName, entry);
            logger.info("registered schedule for pipeline: {}, cron: {}", pipelineName, cronExpression);
        } catch (Exception e) {
            logger.error("failed to register schedule for pipeline: " + pipelineName, e);
        }
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
    private void scheduleNextIfActive(String pipelineName) {
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
    private boolean hasRunningInstance(String pipelineName) {
//        WorkFlowBuildHistoryCriteria criteria = new WorkFlowBuildHistoryCriteria();
//        WorkFlowBuildHistoryCriteria.Criteria c = criteria.createCriteria();
//        c.andAppNameEqualTo(pipelineName);

        return buildHistoryDAO.hasRunningInstance(DataXName.createDataXPipeline(pipelineName));

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
     * Internal record for an active schedule entry.
     */
    private static class ScheduleEntry {
        final String pipelineName;
        final StoreResourceType resType;
        final String cronExpression;
        Cancellable scheduledTask;

        ScheduleEntry(String pipelineName, StoreResourceType resType, String cronExpression) {
            this.pipelineName = pipelineName;
            this.resType = resType;
            this.cronExpression = cronExpression;
        }
    }
}
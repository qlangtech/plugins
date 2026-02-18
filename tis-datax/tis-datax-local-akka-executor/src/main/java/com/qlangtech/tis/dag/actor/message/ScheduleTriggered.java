package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.StoreResourceType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Message sent by Akka Scheduler to DAGSchedulerActor when a cron schedule fires.
 * Identifies which pipeline's cron has triggered.
 *
 * @author baisui
 */
public final class ScheduleTriggered implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String pipelineName;
    private final StoreResourceType resType;

    public ScheduleTriggered(String pipelineName, StoreResourceType resType) {
        this.pipelineName = Objects.requireNonNull(pipelineName, "pipelineName can not be null");
        this.resType = Objects.requireNonNull(resType, "resType can not be null");
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public StoreResourceType getResType() {
        return resType;
    }

    public DataXName getDataName() {
        return new DataXName(pipelineName, resType);
    }

    @Override
    public String toString() {
        return "ScheduleTriggered{pipelineName='" + pipelineName + "', resType=" + resType + "}";
    }
}
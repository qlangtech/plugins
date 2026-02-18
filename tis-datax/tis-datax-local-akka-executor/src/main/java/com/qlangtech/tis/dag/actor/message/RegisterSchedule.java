package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.StoreResourceType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Message to dynamically register a new cron schedule for a pipeline.
 *
 * @author baisui
 */
public final class RegisterSchedule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String pipelineName;
    private final StoreResourceType resType;
    private final String cronExpression;

    public RegisterSchedule(String pipelineName, StoreResourceType resType, String cronExpression) {
        this.pipelineName = Objects.requireNonNull(pipelineName, "pipelineName can not be null");
        this.resType = Objects.requireNonNull(resType, "resType can not be null");
        this.cronExpression = Objects.requireNonNull(cronExpression, "cronExpression can not be null");
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public StoreResourceType getResType() {
        return resType;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    @Override
    public String toString() {
        return "RegisterSchedule{pipelineName='" + pipelineName + "', resType=" + resType
                + ", cron='" + cronExpression + "'}";
    }
}
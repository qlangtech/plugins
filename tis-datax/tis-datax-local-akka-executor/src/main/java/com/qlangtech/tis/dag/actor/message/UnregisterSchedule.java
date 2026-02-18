package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.StoreResourceType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Message to dynamically remove a cron schedule for a pipeline.
 *
 * @author baisui
 */
public final class UnregisterSchedule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String pipelineName;
    private final StoreResourceType resType;

    public UnregisterSchedule(String pipelineName, StoreResourceType resType) {
        this.pipelineName = Objects.requireNonNull(pipelineName, "pipelineName can not be null");
        this.resType = Objects.requireNonNull(resType, "resType can not be null");
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public StoreResourceType getResType() {
        return resType;
    }

    @Override
    public String toString() {
        return "UnregisterSchedule{pipelineName='" + pipelineName + "', resType=" + resType + "}";
    }
}

package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * Singleton message sent to DAGSchedulerActor on startup to trigger loading all scheduled workflows.
 *
 * @author baisui
 */
public final class LoadSchedules implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final LoadSchedules INSTANCE = new LoadSchedules();

    private LoadSchedules() {
    }
}
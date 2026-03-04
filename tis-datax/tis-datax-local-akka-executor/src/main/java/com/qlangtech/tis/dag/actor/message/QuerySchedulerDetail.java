package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * Message sent to DAGSchedulerActor to query all scheduled crontab entries.
 *
 * @author baisui
 */
public class QuerySchedulerDetail implements Serializable {
    private static final long serialVersionUID = 1L;
}

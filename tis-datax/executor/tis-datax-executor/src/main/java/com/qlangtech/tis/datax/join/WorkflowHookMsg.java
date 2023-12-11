package com.qlangtech.tis.datax.join;

import com.qlangtech.tis.datax.JobHookMsg;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/7
 */
public class WorkflowHookMsg extends JobHookMsg {

    private String workflowName;
    private ISqlTask sqlTask;
    private TabPartitions dptPts;


    public static WorkflowHookMsg create(ISqlTask sqlTask, DefaultExecContext execContext, String jobName) {
        //        String resName,
        //        StoreResourceType resType,
        //        Integer taskId, String jobName,
        //                Long currentTimeStamp, Supplier<T> creator)
        String workflowName = execContext.getWorkflowName();
        Integer taskId = execContext.getTaskId();
        Long currentTimeStamp = execContext.getPartitionTimestampWithMillis();

        TabPartitions dptPts = ExecChainContextUtils.getDependencyTablesPartitions(execContext);

        //        dptPts.visitAll((entry) -> {
        //            IDumpTable tab = entry.getKey();
        //            ITabPartition ps = entry.getValue();
        //        });

        return createHookMsg(workflowName, StoreResourceType.DataFlow, taskId, jobName, currentTimeStamp,
                execContext.isDryRun(), () -> {
            WorkflowHookMsg hookMsg = new WorkflowHookMsg();
            hookMsg.setWorkflowName(workflowName);
            hookMsg.sqlTask = Objects.requireNonNull(sqlTask);
            hookMsg.dptPts = Objects.requireNonNull(dptPts, "dptPts can not be null");
            return hookMsg;
        });
    }

    public ISqlTask getSqlTask() {
        return sqlTask;
    }

    public TabPartitions getDptPts() {
        return this.dptPts;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }
}

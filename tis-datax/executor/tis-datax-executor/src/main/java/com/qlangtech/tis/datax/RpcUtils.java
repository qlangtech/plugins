package com.qlangtech.tis.datax;

import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.tis.hadoop.rpc.StatusRpcClientFactory;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/8
 */
public class RpcUtils {
    private RpcUtils() {

    }

    public static void setJoinStatus(Integer taskId, boolean complete, boolean faild,
                                     StatusRpcClientFactory.AssembleSvcCompsite svc, String taskName) {
        JoinPhaseStatus.JoinTaskStatus joinStatus = new JoinPhaseStatus.JoinTaskStatus(taskName);
        joinStatus.setComplete(complete);
        joinStatus.setFaild(faild);
        joinStatus.setStart();
        svc.reportJoinStatus(taskId, joinStatus);
    }
}

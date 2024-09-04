package com.qlangtech.tis.datax;

import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.solrj.util.ZkUtils;
import org.apache.commons.exec.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * used by com.qlangtech.tis.plugin.datax.TaskExec
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/18
 */
public abstract class DataxSplitTabSyncConsumer extends DataXJobSingleProcessorExecutor<CuratorDataXTaskMessage> {
    private static final Logger logger = LoggerFactory.getLogger(DataxSplitTabSyncConsumer.class);
    protected IExecChainContext execContext;


    public DataxSplitTabSyncConsumer(IExecChainContext execContext) {
        this.execContext = execContext;
    }

    @Override
    protected final DataXJobSubmit.InstanceType getExecMode() {
        return DataXJobSubmit.InstanceType.LOCAL;
    }


    @Override
    protected final boolean useRuntimePropEnvProps() {
        return false;
    }

    //    @Override
    //    protected String[] getExtraJavaSystemPrams() {
    //        return new String[]{"-D" + CenterResource.KEY_notFetchFromCenterRepository + "=true"};
    //    }

    @Override
    protected final String getIncrStateCollectAddress() {
        return ZkUtils.getFirstChildValue(Objects.requireNonNull(execContext.getZkClient(),
                "getZkClient can not be " + "null"), ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH);
    }

    /**
     * @see com.qlangtech.tis.datax.DataxExecutor#main(String[])
     * @param msg
     * @param taskId
     * @param jobName
     * @param dataxName
     * @param cmdLine
     */
    @Override
    protected void addMainClassParams(CuratorDataXTaskMessage msg, Integer taskId, String jobName, String dataxName,
                                      CommandLine cmdLine) {


        StoreResourceType resType = Objects.requireNonNull(msg.getResType(), "resType can not be null");
        Integer allRowsApproximately = msg.getAllRowsApproximately();
        logger.info("process DataX job, dataXName:{},jobid:{},jobName:{},allrows:{}", dataxName, taskId, jobName, allRowsApproximately);
        cmdLine.addArgument(String.valueOf(taskId));
        cmdLine.addArgument(jobName);
        // 需要一个占位
        cmdLine.addArgument(dataxName);
        //  cmdLine.addArgument(jobPath, true);
        cmdLine.addArgument(getIncrStateCollectAddress());
        // 执行模式
        cmdLine.addArgument(getExecMode().literia);
        // 估计 总记录数目
        cmdLine.addArgument(String.valueOf(allRowsApproximately));
        // 当前批次的执行时间戳
        // cmdLine.addArgument(msg.getExecTimeStamp());
        // 存储资源类型
        cmdLine.addArgument(resType.getType());

        cmdLine.addArgument(String.valueOf(msg.getTaskSerializeNum()));
        cmdLine.addArgument(String.valueOf(msg.getExecEpochMilli()));
        cmdLine.addArgument(String.valueOf(msg.isDisableGrpcRemoteServerConnect()));
        //  cmdLine.addArgument(msg.)
    }
}

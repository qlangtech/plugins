package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXJobSubmitParams;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.tis.hadoop.rpc.RpcServiceReference;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/21
 */
public class SplitTabSync {
    public DataXJobInfo tskMsg;
    private final Integer allRows;
    // private CuratorDataXTaskMessage taskConfig;

//    public SplitTabSync() {
//    }

    public SplitTabSync(DataXJobInfo tskMsg, Integer allRows) {
        this.tskMsg = tskMsg;
        this.allRows = allRows;
        //  this.taskConfig = Objects.requireNonNull(taskConfig);
    }

    public IRemoteTaskTrigger createTrigger(final AbstractExecContext execChainContext, RpcServiceReference statusRpc) {
        if (statusRpc == null) {
            throw new IllegalArgumentException("statusRpc can not be null");
        }
        DataXJobSubmitParams submitParams = DataXJobSubmitParams.getDftIfEmpty();
        DataXJobSubmit dataXJobSubmit = submitParams.getTaskSubmit(execChainContext.isDryRun());

        if (dataXJobSubmit instanceof DataXJobRunEnvironmentParamsSetter) {
            DataXJobRunEnvironmentParamsSetter runEnvironmentParamsSetter =
                    (DataXJobRunEnvironmentParamsSetter) dataXJobSubmit;
            DataxPrePostConsumer prePostConsumer = BasicTISTableDumpProcessor.createPrePostConsumer();
            runEnvironmentParamsSetter.setClasspath(prePostConsumer.getClasspath());
            // runEnvironmentParamsSetter.setWorkingDirectory(prePostConsumer.getWorkingDirectory());
            runEnvironmentParamsSetter.setExtraJavaSystemPramsSuppiler(prePostConsumer.getExtraJavaSystemPramsSuppiler());
        }

        DataXJobSubmit.IDataXJobContext dataXJobContext = DataXJobSubmit.IDataXJobContext.create(execChainContext);
        IDataxProcessor processor = execChainContext.getProcessor();
        CuratorDataXTaskMessage taskCfg = dataXJobSubmit.getDataXJobDTO(dataXJobContext, tskMsg, processor, this.allRows);
        return dataXJobSubmit.createDataXJob(dataXJobContext, statusRpc, tskMsg, processor, taskCfg);
    }

    public void execSync(final AbstractExecContext execChainContext, RpcServiceReference statusRpc) {
        createTrigger(execChainContext, statusRpc).run();
    }

//    private static DataXJobSubmit getDataXJobSubmit(AbstractExecContext execChainContext) {
//        DataXJobSubmit.InstanceType instanceType = (TisAppLaunch.isTestMock() && false) ? DataXJobSubmit.InstanceType.EMBEDDED :
//                DataXJobSubmit.InstanceType.LOCAL;
//
//        Optional<DataXJobSubmit> dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit(execChainContext.isDryRun(),
//                instanceType);
//        if (dataXJobSubmit.isEmpty()) {
//            throw new IllegalStateException("dataXJobSubmit must be present ,instanceType:"
//                    + instanceType + ",isDryRun:" + execChainContext.isDryRun());
//        }
//        return dataXJobSubmit.get();
//    }


}

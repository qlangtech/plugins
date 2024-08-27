package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.tis.hadoop.rpc.RpcServiceReference;

import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/21
 */
public class SplitTabSync {
    public CuratorDataXTaskMessage tskMsg;

    public SplitTabSync() {
    }

    public SplitTabSync(CuratorDataXTaskMessage tskMsg) {
        this.tskMsg = tskMsg;
    }

    public void execSync(final DefaultExecContext execChainContext, RpcServiceReference statusRpc) {

        DataXJobSubmit dataXJobSubmit = getDataXJobSubmit(execChainContext);
        if (dataXJobSubmit instanceof DataXJobRunEnvironmentParamsSetter) {
            DataXJobRunEnvironmentParamsSetter runEnvironmentParamsSetter =
                    (DataXJobRunEnvironmentParamsSetter) dataXJobSubmit;
            DataxPrePostConsumer prePostConsumer = BasicTISTableDumpProcessor.createPrePostConsumer();// new
            // DataxPrePostConsumer();
            runEnvironmentParamsSetter.setClasspath(prePostConsumer.getClasspath());
            runEnvironmentParamsSetter.setWorkingDirectory(prePostConsumer.getWorkingDirectory());
            runEnvironmentParamsSetter.setExtraJavaSystemPramsSuppiler(prePostConsumer.getExtraJavaSystemPramsSuppiler());
        }

        DataXJobSubmit.IDataXJobContext dataXJobContext = DataXJobSubmit.IDataXJobContext.create(execChainContext);
        IDataxProcessor processor = execChainContext.getProcessor();
        IRemoteTaskTrigger tskTrigger = dataXJobSubmit.createDataXJob(dataXJobContext, statusRpc,
                DataXJobInfo.parse(tskMsg.getJobName()), processor, tskMsg);

        tskTrigger.run();
    }

    private static DataXJobSubmit getDataXJobSubmit(DefaultExecContext execChainContext) {
        DataXJobSubmit.InstanceType instanceType = TisAppLaunch.isTestMock() ? DataXJobSubmit.InstanceType.EMBEDDED :
                DataXJobSubmit.InstanceType.LOCAL;

        Optional<DataXJobSubmit> dataXJobSubmit = DataXJobSubmit.getDataXJobSubmit(execChainContext.isDryRun(),
                instanceType);
        if (!dataXJobSubmit.isPresent()) {
            throw new IllegalStateException("dataXJobSubmit must be present ,instanceType:"
                    + instanceType + ",isDryRun:" + execChainContext.isDryRun());
        }
        return dataXJobSubmit.get();
    }


}

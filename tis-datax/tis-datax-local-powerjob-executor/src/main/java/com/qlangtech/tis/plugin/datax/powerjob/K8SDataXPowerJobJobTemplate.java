package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.BasicPowerjobWorker;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.trigger.jst.ILogListener;
import tech.powerjob.common.enums.DispatchStrategy;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.ProcessorType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.http.SaveJobInfoRequest;

/**
 * 配置PowerJob Worker执行器
 * https://www.yuque.com/powerjob/guidence/ysug77#mNarp
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class K8SDataXPowerJobJobTemplate extends BasicPowerjobWorker {


    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer instraceRetry;

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxInstance;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer taskRetry;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer threadParallel;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer timeLimit;


    //    ● 集群配置
//  ○ 执行机器地址，指定集群中的某几台机器执行任务
//    ■ IP模式：多值英文逗号分割，如192.168.1.1:27777,192.168.1.2:27777。常用于 debug 等场景，需要指定特定机器运行。
//            ■ TAG 模式：通过 PowerJobWorkerConfig#tag将执行器打标分组后，可在控制台通过 tag 指定某一批机器执行。常用于分环境分单元执行的场景。如某些任务需要屏蔽安全生产环境（tag 设置为环境标），某些任务只需要在特定单元执行（tag 设置单元标）
//            ○ 最大执行机器数量：限定调动执行的机器数量
    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxWorkerParallel;


    public SaveJobInfoRequest createDefaultJobInfoRequest(ExecuteType executeType) {
        SaveJobInfoRequest saveJobInfoReq = new SaveJobInfoRequest();
        saveJobInfoReq.setEnable(true);
        saveJobInfoReq.setInstanceRetryNum(this.instraceRetry);
        saveJobInfoReq.setMaxInstanceNum(this.maxInstance);
        saveJobInfoReq.setTaskRetryNum(this.taskRetry);
        saveJobInfoReq.setConcurrency(this.threadParallel);
        saveJobInfoReq.setInstanceTimeLimit(this.timeLimit.longValue());
        saveJobInfoReq.setMaxWorkerCount(maxWorkerParallel);
        saveJobInfoReq.setDispatchStrategy(DispatchStrategy.HEALTH_FIRST);
        saveJobInfoReq.setExecuteType(executeType);
        saveJobInfoReq.setTimeExpressionType(TimeExpressionType.API);

        saveJobInfoReq.setProcessorType(ProcessorType.BUILT_IN);
        saveJobInfoReq.setProcessorInfo("com.qlangtech.tis.datax.powerjob.TISTableDumpProcessor");
        return saveJobInfoReq;
    }


    @Override
    public void relaunch() {

    }

    @Override
    public void relaunch(String podName) {

    }

    @Override
    public RcDeployment getRCDeployment() {
        return null;
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return null;
    }


    @Override
    public void remove() {

    }

    @Override
    public void launchService(Runnable launchProcess) {

    }

    @TISExtension()
    public static class DescriptorImpl extends BasicDescriptor {

        public DescriptorImpl() {
            super();
        }

        @Override
        protected K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.JobTpl;
        }

        @Override
        protected TargetResName getWorkerType() {
            return K8S_DATAX_INSTANCE_NAME;
        }
    }
}

package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.BasicPowerjobWorker;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import tech.powerjob.common.enums.DispatchStrategy;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.ProcessorType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.enums.WorkflowNodeType;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.request.http.SaveWorkflowRequest;

import java.util.Objects;
import java.util.Optional;

/**
 * 配置PowerJob Worker执行器
 * https://www.yuque.com/powerjob/guidence/ysug77#mNarp
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class K8SDataXPowerJobJobTemplate extends BasicPowerjobWorker {

    public static K8SDataXPowerJobJobTemplate getAppRelevantDataXJobWorkerTemplate(IDataxProcessor dataxProcessor) {
        for (DataXJobWorker worker : HeteroEnum.appJobWorkerTplReWriter.getPlugins(IPluginContext.namedContext(dataxProcessor.identityValue()), null)) {
            return (K8SDataXPowerJobJobTemplate) worker;
        }

        // 为空,调用全局模版
        return (K8SDataXPowerJobJobTemplate) getJobWorker(
                TargetResName.K8S_DATAX_INSTANCE_NAME, Optional.of(K8SWorkerCptType.JobTpl));
    }

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer instraceRetry;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxInstance;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer taskRetry;

    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer threadParallel;

    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer timeLimit;


    //    ● 集群配置
//  ○ 执行机器地址，指定集群中的某几台机器执行任务
//    ■ IP模式：多值英文逗号分割，如192.168.1.1:27777,192.168.1.2:27777。常用于 debug 等场景，需要指定特定机器运行。
//            ■ TAG 模式：通过 PowerJobWorkerConfig#tag将执行器打标分组后，可在控制台通过 tag 指定某一批机器执行。常用于分环境分单元执行的场景。如某些任务需要屏蔽安全生产环境（tag 设置为环境标），某些任务只需要在特定单元执行（tag 设置单元标）
//            ○ 最大执行机器数量：限定调动执行的机器数量
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxWorkerParallel;

    /**
     * workflow 节点执行过程中，如果失败是否跳过继续执行下游节点？
     */
    @FormField(ordinal = 13, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean skipWhenFailed;


    public SaveJobInfoRequest createSqlProcessJobRequest() {
        return createDefaultJobInfoRequest(ExecuteType.STANDALONE, "com.qlangtech.tis.datax.powerjob.TISTableJoinProcessor");
    }

    public SaveJobInfoRequest createSynJobRequest() {
        return createDefaultJobInfoRequest(ExecuteType.MAP_REDUCE, "com.qlangtech.tis.datax.powerjob.TISTableDumpProcessor");
    }

    public SaveJobInfoRequest createInitializeJobRequest() {
        return createDefaultJobInfoRequest(ExecuteType.STANDALONE, "com.qlangtech.tis.datax.powerjob.TISInitializeProcessor");
    }

    public SaveJobInfoRequest createDefaultJobInfoRequest(ExecuteType executeType, String targetClass) {
        SaveJobInfoRequest saveJobInfoReq = new SaveJobInfoRequest();
        saveJobInfoReq.setEnable(true);
        saveJobInfoReq.setInstanceRetryNum(this.instraceRetry);
        saveJobInfoReq.setMaxInstanceNum(this.maxInstance);
        saveJobInfoReq.setTaskRetryNum(this.taskRetry);
        saveJobInfoReq.setConcurrency(this.threadParallel);
        saveJobInfoReq.setInstanceTimeLimit(this.timeLimit.longValue());
        saveJobInfoReq.setMaxWorkerCount(maxWorkerParallel);
        saveJobInfoReq.setDispatchStrategy(DispatchStrategy.RANDOM);
        saveJobInfoReq.setExecuteType(executeType);
        //   工作流 -> 不需要填写任何参数，表明该任务由工作流（workflow）触发
        saveJobInfoReq.setTimeExpressionType(TimeExpressionType.WORKFLOW);

        saveJobInfoReq.setProcessorType(ProcessorType.BUILT_IN);
        saveJobInfoReq.setProcessorInfo(targetClass);
        return saveJobInfoReq;
    }


    public SaveWorkflowRequest createWorkflowRequest(IDataxProcessor dataxProcessor) {
        SaveWorkflowRequest req = new SaveWorkflowRequest();
        req.setWfName(dataxProcessor.identityValue());
        req.setTimeExpressionType(TimeExpressionType.API);
        req.setMaxWfInstanceNum(this.maxInstance);
        req.setWfDescription(dataxProcessor.identityValue());
        req.setEnable(true);
        return req;
    }

    /**
     * 创建workflow node 节点
     *
     * @return
     */
    public SaveWorkflowNodeRequest createWorkflowNode() {
        SaveWorkflowNodeRequest wfNode = new SaveWorkflowNodeRequest();
        wfNode.setSkipWhenFailed(Objects.requireNonNull(this.skipWhenFailed, "skipWhenFailed  can not be null"));
        wfNode.setEnable(true);
        wfNode.setType(WorkflowNodeType.JOB.getCode());
        return wfNode;
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicDescriptor implements IEndTypeGetter {

        public DescriptorImpl() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.PowerJob;
        }

        @Override
        public K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.JobTpl;
        }

        @Override
        protected TargetResName getWorkerType() {
            return TargetResName.K8S_DATAX_INSTANCE_NAME;
        }
    }
}

package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import tech.powerjob.common.request.http.SaveWorkflowRequest;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/25
 */
public class K8SDataXPowerJobOverwriteTemplate extends K8SDataXPowerJobJobTemplate  {

    @FormField(ordinal = 0, validate = {Validator.require})
    public TriggerStrategy triggerStrategy;

    @Override
    public SaveWorkflowRequest createWorkflowRequest(IDataxProcessor dataxProcessor) {
        SaveWorkflowRequest req = super.createWorkflowRequest(dataxProcessor);
//        req.setWfName(dataxProcessor.identityValue());
//        req.setWfDescription(dataxProcessor.identityValue());
//        req.setEnable(true);
        Objects.requireNonNull(this.triggerStrategy, "triggerStrategy can not be null").setTimeExpression(req);
        return req;
    }

//    @Override
//    public void afterSaved() {
//        PowerJobClient powerJob = DistributedPowerJobDataXJobSubmit.getTISPowerJob();
//
//        DistributedPowerJobDataXJobSubmit.innerSaveJob( );
//    }

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
        public  K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.JobTplAppOverwrite;
        }

        @Override
        protected TargetResName getWorkerType() {
            return K8S_DATAX_INSTANCE_NAME;
        }
    }
}

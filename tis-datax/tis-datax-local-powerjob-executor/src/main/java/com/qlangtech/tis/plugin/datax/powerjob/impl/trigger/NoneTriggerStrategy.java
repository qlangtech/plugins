package com.qlangtech.tis.plugin.datax.powerjob.impl.trigger;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.TriggerStrategy;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.http.SaveWorkflowRequest;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/25
 */
public class NoneTriggerStrategy extends TriggerStrategy {

    @Override
    public void setTimeExpression(SaveWorkflowRequest workflowRequest) {
        workflowRequest.setTimeExpressionType(TimeExpressionType.API);
    }

    @TISExtension
    public static final class DftDesc extends Descriptor<TriggerStrategy> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}

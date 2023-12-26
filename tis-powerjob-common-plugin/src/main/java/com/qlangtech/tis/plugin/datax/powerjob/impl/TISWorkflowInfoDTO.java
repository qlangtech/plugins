package com.qlangtech.tis.plugin.datax.powerjob.impl;

import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.response.WorkflowInfoDTO;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/28
 */
public class TISWorkflowInfoDTO extends WorkflowInfoDTO {

    public String getCronInfo() {

        TimeExpressionType triggerType = TimeExpressionType.valueOf(this.getTimeExpressionType());
        switch (triggerType) {
            case API:
                return "none";
            case CRON:
                return this.getTimeExpression();
            case FIXED_RATE:
            case FIXED_DELAY:
            case DAILY_TIME_INTERVAL:
            case WORKFLOW:
            default:
                throw new IllegalStateException("triggerType have not support");
        }
    }

}

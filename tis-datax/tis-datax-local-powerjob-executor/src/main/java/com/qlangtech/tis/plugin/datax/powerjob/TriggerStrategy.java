package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtensible;
import tech.powerjob.common.request.http.SaveWorkflowRequest;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/25
 */
@TISExtensible
public abstract class TriggerStrategy implements Describable<TriggerStrategy> {


 public abstract void setTimeExpression(SaveWorkflowRequest workflowRequest );

//    SaveWorkflowRequest req = new SaveWorkflowRequest();
//        req.setWfName(dataxProcessor.identityValue());
//        req.setWfDescription(dataxProcessor.identityValue());
//        req.setEnable(true);
//        req.setTimeExpressionType(TimeExpressionType.API);
//        req.setTimeExpression( );
}

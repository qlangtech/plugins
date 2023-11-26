package com.qlangtech.tis.plugin.datax.powerjob.impl.trigger;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.TriggerStrategy;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.quartz.CronScheduleBuilder;
import org.quartz.spi.MutableTrigger;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.http.SaveWorkflowRequest;

import java.util.Date;
import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/25
 */
public class CrontabTriggerStrategy extends TriggerStrategy {
    private static final String KEY_CRONTAB = "crontab";
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String crontab;

    @Override
    public void setTimeExpression(SaveWorkflowRequest workflowRequest) {
        workflowRequest.setTimeExpressionType(TimeExpressionType.CRON);
        workflowRequest.setTimeExpression(crontab);
    }

    @TISExtension
    public static final class DftDesc extends Descriptor<TriggerStrategy> {
        @Override
        public String getDisplayName() {
            return "Crontab";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // return super.verify(msgHandler, context, postFormVals);
            //   CrontabTriggerStrategy describable = postFormVals.newInstance();
            Date nextFireTime = getNextFireTime(msgHandler, context, KEY_CRONTAB, postFormVals.getField(KEY_CRONTAB));
            if (nextFireTime != null) {
                msgHandler.addActionMessage(context, "最近触发时间为：" + TimeFormat.yyyyMMddHHmmss.format(nextFireTime.getTime()));
                return true;
            }
            return false;
        }

        public boolean validateCrontab(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Date nextFireTime = getNextFireTime(msgHandler, context, fieldName, value);
            return nextFireTime != null;
        }

        private Date getNextFireTime(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(value);
                MutableTrigger build = scheduleBuilder.build();
                Date nextFireTime = Objects.requireNonNull(build.getFireTimeAfter(new Date()), "getFireTimeAfter can not be null");
                return nextFireTime;
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return null;
            }
        }


    }
}

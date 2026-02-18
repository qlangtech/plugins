package com.qlangtech.tis.dag;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.Date;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/16
 * //@see CrontabTriggerStrategy
 */
public class BatchJobCrontab extends DefaultDataXProcessorManipulate implements IManipulateStatus, IdentityDesc<JSONObject> {
    public static final String KEY_CRONTAB = "crontab";
    private transient int triggerCount;
    private transient long lastTriggerTimestamp = -1;
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String crontab;
    /**
     * 是否启效
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean turnOn;


    @Override
    public JSONObject describePlugin() {
        return Descriptor.getManipulateMeta(false, this);
    }

    @Override
    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context, ManipulateItemsProcessor itemsProcessor) {

    }

    @Override
    public ManipulateStateSummary manipulateStatusSummary() {
        final StringBuilder summary = new StringBuilder("当前处于‘" + (turnOn ? "激活" : "禁用") + "’状态");
        if (turnOn) {
            summary.append("，已经触发了").append(this.triggerCount).append("次执行");
            if (this.lastTriggerTimestamp > 0) {
                summary.append("，最近触发时间：").append(TimeFormat.yyyyMMdd_HH_mm_ss.format(this.lastTriggerTimestamp));
            }
        }
        return new ManipulateStateSummary(
                Collections.singletonList(IManipulateStatus.create(String.valueOf(this.triggerCount)))
                , summary.toString(), turnOn);
    }

    @TISExtension
    public static final class DftDesc extends DefaultDataXProcessorManipulate.BasicDesc {
        @Override
        public String getDisplayName() {
            return "Crontab";
        }

        @Override
        public EndType getEndType() {
            return EndType.Crontab;
        }

        @Override
        public boolean isManipulateStorable() {
            return true;
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
//            try {
//                CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(value);
//                MutableTrigger build = scheduleBuilder.build();
//                Date nextFireTime = Objects.requireNonNull(build.getFireTimeAfter(new Date()), "getFireTimeAfter can not be null");
//                return nextFireTime;
//            } catch (Exception e) {
//                msgHandler.addFieldError(context, fieldName, e.getMessage());
//                return null;
//            }
            return new Date();
        }


    }
}

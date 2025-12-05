package com.qlangtech.plugins.incr.flink.alert;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.alert.AlertChannel;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 为当前用户添加报警
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/17
 */
public class AddMonitorForEvents extends DefaultDataXProcessorManipulate
        implements IdentityName, DefaultDataXProcessorManipulate.MonitorForEventsManager, IManipulateStatus, IdentityDesc<JSONObject> {

    private static final String KEY_ALERT_CHANNEL = "alertChannel";
    /**
     * 是否启效
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean turnOn;

    private transient int alertSendCount;
    private transient long lastSendTimestamp = -1;

    /**
     * 选择发送渠道
     */
    @FormField(ordinal = 3, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public List<String> alertChannel;

    @Override
    public boolean isActivate() {
        return this.turnOn;
    }

    @Override
    public JSONObject describePlugin() {
        return Descriptor.getManipulateMeta(false, this);
    }

    /**
     * 在前端tag中可以显示执行状态
     *
     * @return
     */
    @Override
    public ManipulateStateSummary manipulateStatusSummary() {
        final StringBuilder summary = new StringBuilder("当前处于‘" + (turnOn ? "激活" : "禁用") + "’状态");
        if (turnOn) {
            summary.append("，已经向消息通道发送").append(this.alertSendCount).append("条报警消息");
            if (this.lastSendTimestamp > 0) {
                summary.append("，最近发送时间：").append(TimeFormat.yyyyMMdd_HH_mm_ss.format(this.lastSendTimestamp));
            }
        }
        return new ManipulateStateSummary(
                Collections.singletonList(IManipulateStatus.create(String.valueOf(this.alertSendCount)))
                , summary.toString(), turnOn);
    }

    @Override
    public void addSendCount() {
        alertSendCount++;
        this.lastSendTimestamp = TimeFormat.getCurrentTimeStamp();
    }

    @Override
    public List<AlertChannel> getAlertChannels() {
        return AlertChannel.load(Sets.newHashSet(alertChannel));
    }

    @Override
    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context, ManipulateItemsProcessor itemsProcessor) {

    }

//    @Override
//    public String identityValue() {
//        return this.name;
//    }

    @TISExtension
    public static class DefaultDesc extends DefaultDataXProcessorManipulate.BasicDesc implements IEndTypeGetter {
        public DefaultDesc() {
            super();
            this.registerSelectOptions(KEY_ALERT_CHANNEL, () -> ParamsConfig.getItems(AlertChannel.KEY_CATEGORY));
        }

        public boolean validateName(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (!MonitorForEventsManager.KEY_ALERT.equals(value)) {
                msgHandler.addFieldError(context, fieldName, "名称必须为：" + MonitorForEventsManager.KEY_ALERT);
                return false;
            }
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Alert;
        }

        @Override
        public boolean isManipulateStorable() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Add Monitor for Alert";
        }
    }
}

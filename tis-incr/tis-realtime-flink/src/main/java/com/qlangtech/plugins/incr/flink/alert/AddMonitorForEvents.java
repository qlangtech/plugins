package com.qlangtech.plugins.incr.flink.alert;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.alert.AlertChannel;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Optional;

/**
 * 为当前用户添加报警
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/17
 */
public class AddMonitorForEvents extends DefaultDataXProcessorManipulate implements IdentityName {

    private static final String KEY_ALERT_CHANNEL = "alertChannel";

    @FormField(ordinal = 1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity, Validator.forbid_start_with_number})
    public String name;

    /**
     * 是否启效
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean turnOn;

    /**
     * 选择发送渠道
     */
    @FormField(ordinal = 3, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public List<String> alertChannel;


//    @Override
//    public void manipuldateProcess(IPluginContext pluginContext, Optional<Context> context) {
//        // 将TIS的数据同步通道配置同步到DS中
//        // String[] originId = new String[1];
//        /**
//         * 校验
//         */
//        ManipulateItemsProcessor itemsProcessor
//                = ManipuldateUtils.instance(pluginContext, context.get(), null
//                , (meta) -> {
//                });
//        if (StringUtils.isEmpty(itemsProcessor.getOriginIdentityId())) {
//            throw new IllegalStateException("originId can not be null");
//        }
//        if (itemsProcessor == null) {
//            return;
//        }
//
//        Pair<List<AddMonitorForEvents>, IPluginStore<DefaultDataXProcessorManipulate>>
//                pair = DefaultDataXProcessorManipulate.loadPlugins(pluginContext
//                , AddMonitorForEvents.class, DataXName.createDataXPipeline(itemsProcessor.getOriginIdentityId()));
//
//        /**
//         * 是否需要删除
//         */
//        if (itemsProcessor.isDeleteProcess()) {
//            pair.getRight().setPlugins(pluginContext, context, Collections.emptyList());
//            return;
//        }
//
//
//        if (!itemsProcessor.isUpdateProcess()) {
//            // 添加操作
//            if (pair.getLeft().size() > 0) {
//                for (AddMonitorForEvents i : pair.getLeft()) {
//                    pluginContext.addErrorMessage(context.get(), "报警实例'" + i.name + "'已经配置，不能再创建新实例");
//                }
//                //  throw TisException.create("实例已经配置不能重复创建");
//                return;
//            }
//        }
//
//        /**
//         *2. 并且将实例持久化在app管道下，当DS端触发会调用 DolphinschedulerDistributedSPIDataXJobSubmit.createPayload()方法获取DS端的WorkflowDAG拓扑视图
//         */
//        pair.getRight().setPlugins(pluginContext, context, Collections.singletonList(new Descriptor.ParseDescribable(this)));
//    }

    @Override
    protected void afterManipuldateProcess(IPluginContext pluginContext, Optional<Context> context, ManipulateItemsProcessor itemsProcessor) {

    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DefaultDesc extends DefaultDataXProcessorManipulate.BasicDesc implements IEndTypeGetter {
        public DefaultDesc() {
            super();
            this.registerSelectOptions(KEY_ALERT_CHANNEL, () -> ParamsConfig.getItems(AlertChannel.KEY_CATEGORY));
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

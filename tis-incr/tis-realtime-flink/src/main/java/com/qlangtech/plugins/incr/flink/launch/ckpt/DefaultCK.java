package com.qlangtech.plugins.incr.flink.launch.ckpt;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.launch.CheckpointFactory;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/24
 */
public class DefaultCK extends CheckpointFactory {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer ckpointInterval;

    @Override
    public final boolean isOn() {
        return true;
    }

    @Override
    public void setProps(StreamExecutionEnvironment env) {
        env.enableCheckpointing(Duration.ofSeconds(this.ckpointInterval).toMillis());
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<CheckpointFactory> {
        protected final FlinkPropAssist.Options<CheckpointFactory> opts = FlinkPropAssist.createOpts(this);

        public DefaultDescriptor() {
            super();
            opts.addFieldDescriptor("ckpointInterval"
                    , ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL
                    , OverwriteProps.dft(Duration.ofSeconds(200)).setReadOnly(this.ckpointIntervalReadOnly()));
        }

        protected boolean ckpointIntervalReadOnly() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        private static final int MIN_INTERVAL = 5;

        public final boolean validateCkpointInterval(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int interval = 0;
            try {
                interval = Integer.parseInt(val);
            } catch (Throwable e) {

            }
            if (interval < MIN_INTERVAL) {
                msgHandler.addFieldError(context, fieldName, "不能小于最小值：" + MIN_INTERVAL);
                return false;
            }
            return true;

        }
    }
}

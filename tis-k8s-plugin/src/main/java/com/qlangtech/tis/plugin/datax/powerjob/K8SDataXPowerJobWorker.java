package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Lists;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.BasicPowerjobWorker;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.trigger.jst.ILogListener;

import java.util.List;

/**
 * 配置PowerJob Worker执行器
 * https://www.yuque.com/powerjob/guidence/deploy_worker
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class K8SDataXPowerJobWorker extends BasicPowerjobWorker {


    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String appName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.host})
    public String serverAddress;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer port;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public String storeStrategy;

    public static List<Option> getStoreStrategy() {
        List<Option> opts = Lists.newArrayList();
        for (StoreStrategy ss : StoreStrategy.values()) {
            opts.add(ss.createOpt());
        }

        return opts;
    }

//    @FormField(ordinal = 7, type = FormFieldType.ENUM, validate = {Validator.require, Validator.integer})
//    public String protocol;

    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxResultLength;

    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, advance = true, validate = {Validator.require, Validator.identity})
    public String userContext;

    @FormField(ordinal = 13, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxLightweightTaskNum;

    @FormField(ordinal = 15, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxHeavyweightTaskNum;

    @FormField(ordinal = 17, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer healthReportInterval;

    public enum StoreStrategy {
        DISK("磁盘"),
        MEMORY("内存");
        private final String des;

        StoreStrategy(String des) {
            this.des = des;
        }

        private Option createOpt() {
            return new Option(this.des, this.name());
        }
    }


    @Override
    public void relaunch() {

    }

    @Override
    public void relaunch(String podName) {

    }

    @Override
    public RcDeployment getRCDeployment() {
        return null;
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return null;
    }


    @Override
    public void remove() {

    }

    @Override
    public void launchService() {

    }

    @TISExtension()
    public static class DescriptorImpl extends BasicDescriptor {

        public DescriptorImpl() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "powerjob-worker";
        }

        @Override
        protected TargetResName getWorkerType() {
            return K8S_DATAX_INSTANCE_NAME;
        }
    }
}

/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.config.yarn;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.impl.Hadoop020RemoteJobTriggerFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/*
 * @create: 2020-02-08 12:15
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class YarnConfig extends ParamsConfig implements IYarnConfig {

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.host, Validator.require})
    public String rmAddress;
    // SCHEDULER
    @FormField(ordinal = 2, validate = {Validator.host, Validator.require})
    public String schedulerAddress;

    @Override
    public YarnConfiguration createConfigInstance() {
        Thread.currentThread().setContextClassLoader(Hadoop020RemoteJobTriggerFactory.class.getClassLoader());
//        YarnClient yarnClient = YarnClient.createYarnClient();
//        yarnClient.init(getYarnConfig());
//        yarnClient.start();
//        return yarnClient;
        return getYarnConfig();
    }

    private YarnConfiguration getYarnConfig() {
        YarnConfiguration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_ADDRESS, rmAddress);
        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);
//        conf.set(YarnConfiguration.RM_ADMIN_ADDRESS, rmAddress);
//        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddress);

        return conf;
    }

//    @Override
//    public String getName() {
//        return this.name;
//    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ParamsConfig> {

        // private List<YarnConfig> installations;
        @Override
        public String getDisplayName() {
            return "yarn";
        }

        public DefaultDescriptor() {
            super();
            // this.load();
        }
    }
}

/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.launch;


import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.common.FlinkCluster;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.flink.client.program.rest.RestClusterClient;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-16 09:16
 **/
public class TISFlinkCDCStreamFactory extends IncrStreamFactory {

    public static final String NAME_FLINK_CDC = "Flink-CDC";

//    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.identity})
//    public String name = NAME_FLINK_CDC;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String flinkCluster;

//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.host, Validator.require})
//    public String jobManagerAddress;
//
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.identity, Validator.require})
//    public String clusterId;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer parallelism;

    public RestClusterClient getFlinkCluster() {
        FlinkCluster item = getClusterCfg();
        return item.createConfigInstance();
    }

    FlinkCluster getClusterCfg() {
        return ParamsConfig.getItem(this.flinkCluster, FlinkCluster.KEY_DISPLAY_NAME);
    }

    @Override
    public IRCController getIncrSync() {
        FlinkTaskNodeController flinkTaskNodeController = new FlinkTaskNodeController(this);
        //flinkTaskNodeController.setTableStreamHandle(createTableStreamHandle());

        return flinkTaskNodeController;
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage();
    }

    // private BasicFlinkSourceHandle createTableStreamHandle() {
    //   return new TISFlinkSourceHandle();
    //}
//
//    @Override
//    public String identityValue() {
//        return this.name;
//    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IncrStreamFactory> {
        //        @Override
//        public String getId() {
//            return IncrStreamFactory.FLINK_STREM;
//        }
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions("flinkCluster", () -> ParamsConfig.getItems(IFlinkCluster.KEY_DISPLAY_NAME));
        }

        @Override
        public String getDisplayName() {
            return NAME_FLINK_CDC;
        }

        /**
         * 校验并行度
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validateParallelism(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer parallelism = Integer.parseInt(value);
            if (parallelism < 1) {
                msgHandler.addFieldError(context, fieldName, "执行并行度不能小于1");
                return false;
            }
            if (parallelism > 16) {
                msgHandler.addFieldError(context, fieldName, "执行并行度不能大于16");
                return false;
            }
            return true;
        }


    }
}

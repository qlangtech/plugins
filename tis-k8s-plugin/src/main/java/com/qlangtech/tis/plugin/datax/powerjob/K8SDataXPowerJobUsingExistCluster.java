/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax.powerjob;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.BasicPowerjobWorker;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 使用已经存在的Powerjob Cluster机群
 * https://www.yuque.com/powerjob/guidence/deploy_server
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 18:16
 **/
@Public
public class K8SDataXPowerJobUsingExistCluster extends BasicPowerjobWorker {

    private static final Logger logger = LoggerFactory.getLogger(K8SDataXPowerJobUsingExistCluster.class);

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer serverPort;


    @TISExtension()
    public static class DescriptorImpl extends BasicDescriptor {

        public DescriptorImpl() {
            super();
        }


        @Override
        protected TargetResName getWorkerType() {
            return K8S_DATAX_INSTANCE_NAME;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return "powerjob-use-exist-cluster";
        }
    }

}

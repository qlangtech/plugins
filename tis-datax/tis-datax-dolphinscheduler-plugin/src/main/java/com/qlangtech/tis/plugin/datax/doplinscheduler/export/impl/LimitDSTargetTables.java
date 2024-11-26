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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DSTargetTables;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.List;

/**
 * 无限制的表数量选择
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-06 08:33
 **/
public class LimitDSTargetTables extends DSTargetTables {
    static final int limitedTabNumber = 15;
    static final String limitedTabNumberErrorMessage = "同步的目标表数量不能超过" + limitedTabNumber;

    @Override
    public List<IdentityName> getTargetTables() {
        if (this.isExceedMaxTabNumber()) {
            throw TisException.create(limitedTabNumberErrorMessage);
        }
        return this.targetTables;
    }

    public static final String getHelpInfo() {
        return "请选择需要同步到DolphinScheduler的表，最大同步数量" + limitedTabNumber + "张";
    }

    /**
     * 是否超过最大可同步表数量
     *
     * @return
     */
    private boolean isExceedMaxTabNumber() {
        return this.targetTables != null
                && this.targetTables.size() > limitedTabNumber;
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Limited";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(
                IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            LimitDSTargetTables targetTables = postFormVals.newInstance();
            if (targetTables.isExceedMaxTabNumber()) {
                msgHandler.addFieldError(context, KEY_FIELD_TARGET_TABLES, limitedTabNumberErrorMessage);
                return false;
            }
            return true;
        }
    }
}

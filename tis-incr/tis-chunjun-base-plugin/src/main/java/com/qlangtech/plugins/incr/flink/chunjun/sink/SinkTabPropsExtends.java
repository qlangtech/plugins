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

package com.qlangtech.plugins.incr.flink.chunjun.sink;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.IncrSelectedTabExtend;
import com.qlangtech.tis.plugins.incr.flink.connector.mysql.UpdateMode;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-18 11:51
 **/
public class SinkTabPropsExtends extends IncrSelectedTabExtend {

//    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
//    public UpdateMode batchMode;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public UpdateMode incrMode;

    public UpdateMode getIncrMode() {
        return Objects.requireNonNull(incrMode, "incrMode can not be null");
    }

    @Override
    public boolean isSource() {
        return false;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseDescriptor {

//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, SelectedTab postFormVals) {
//
//            SinkTabPropsExtends tab = (SinkTabPropsExtends) postFormVals;
//            boolean success = true;
////            if (!tab.containCol(tab.sourceOrderingField)) {
////                msgHandler.addFieldError(context, KEY_SOURCE_ORDERING_FIELD, "'"
////                        + tab.sourceOrderingField + "'需要在" + SelectedTab.KEY_FIELD_COLS + "中被选中");
////                success = false;
////            }
////
////            if (tab.keyGenerator != null) {
////                for (String field : tab.keyGenerator.getRecordFields()) {
////                    if (!tab.containCol(field)) {
////                        msgHandler.addFieldError(context, KEY_RECORD_FIELD
////                                , "'" + field + "'需要在" + SelectedTab.KEY_FIELD_COLS + "中被选中");
////                        success = false;
////                        break;
////                    }
////                }
////            }
//            return success;
//        }


    }
}

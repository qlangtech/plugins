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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json;

import com.qlangtech.tis.extension.TISExtension;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-29 07:15
 **/
public class SinkJsonFormatFactory extends SourceJsonFormatFactory {

//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT
//            , advance = false, validate = {Validator.db_col_name, Validator.require})
//    public String targetTable;

//    @Override
//    public List<String> parseTargetTabsEntities() {
//        return Collections.singletonList(this.targetTable);
//    }

    @Override
    public List<String> parseTargetTabsEntities() {
        throw new UnsupportedOperationException("sink end not supported");
    }

    @TISExtension
    public static final class SinkDescriptor extends SourceDescriptor {
        public SinkDescriptor() {
            super();
        }

//        @Override
//        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return this.validateAll(msgHandler, context, postFormVals);
//        }
//
//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            DataxReader dataxReader = DataxReader.load((IPluginContext) msgHandler, msgHandler.getCollectionName());
//            List<ISelectedTab> tabs = dataxReader.getSelectedTabs();
//            if (tabs.size() > 1) {
//                msgHandler.a
//                return false;
//            }
//            return true;
//        }

        @Override
        public EndType getEndType() {
            return EndType.SINK;
        }
    }
}

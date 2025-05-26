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

package com.qlangtech.tis.realtime;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 09:22
 **/
public class DTOSourceTagProcessFunction extends SourceProcessFunction<DTO> {
    public static final String KEY_MERGE_ALL_TABS_IN_ONE_BUS = "merge_all_tabs_in_one_bus";

    public static DTOSourceTagProcessFunction createMergeAllTabsInOneBus() {
        return new MergeAllTabsInOneBusProcessFunction();
    }

    public DTOSourceTagProcessFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
        super(tab2OutputTag);
    }

    @Override
    protected String getTableName(DTO record) {
        return record.getTableName();
    }


    static class MergeAllTabsInOneBusProcessFunction extends DTOSourceTagProcessFunction {

        public MergeAllTabsInOneBusProcessFunction() {
            super(Collections.singletonMap(KEY_MERGE_ALL_TABS_IN_ONE_BUS, new OutputTag<DTO>(KEY_MERGE_ALL_TABS_IN_ONE_BUS) {
            }));
        }

        @Override
        protected String getTableName(DTO record) {
            return KEY_MERGE_ALL_TABS_IN_ONE_BUS;
        }
    }
}

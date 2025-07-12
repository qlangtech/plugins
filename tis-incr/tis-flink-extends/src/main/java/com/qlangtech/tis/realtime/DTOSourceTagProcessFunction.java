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

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 09:22
 **/
public class DTOSourceTagProcessFunction extends SourceProcessFunction<DTO> {
    public static final String KEY_MERGE_ALL_TABS_IN_ONE_BUS = "merge_all_tabs_in_one_bus";

    public static TableAlias createAllMergeTableAlias() {
        return TableAlias.create(DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS
                , DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS);
    }


    public static Set<String> createFocusTabs(boolean flinkCDCPipelineEnable, List<ISelectedTab> tabs) {
        return (flinkCDCPipelineEnable
                ? Stream.of(DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS)
                : tabs.stream().map((t) -> t.getName())).collect(Collectors.toSet());
    }

    public static DTOSourceTagProcessFunction create(DataXName dataXName, boolean flinkCDCPipelineEnable, Map<String, OutputTag<DTO>> tab2OutputTag) {
        if (flinkCDCPipelineEnable) {
            if (tab2OutputTag.size() != 1 || !tab2OutputTag.containsKey(KEY_MERGE_ALL_TABS_IN_ONE_BUS)) {
                throw new IllegalStateException("the size of tab2OutputTag must be 1,but now is:" + String.join(",", tab2OutputTag.keySet()));
            }
        }
        return flinkCDCPipelineEnable
                ? new MergeAllTabsInOneBusProcessFunction(dataXName, tab2OutputTag)
                : new DTOSourceTagProcessFunction(dataXName, tab2OutputTag);
    }

    public DTOSourceTagProcessFunction(DataXName dataXName, Map<String, OutputTag<DTO>> tab2OutputTag) {
        super(dataXName, tab2OutputTag);
    }

    @Override
    protected String getTableName(DTO record) {
        return record.getTableName();
    }

    @Override
    protected void increaseNumRecordsMetric(DTO in) {
        if (in.getEventType() == EventType.UPDATE_BEFORE) {
            // 当记录为更新时候会有before，after两条，所以需要将before那条记录过滤掉
            return;
        }
        super.increaseNumRecordsMetric(in);
    }

    static class MergeAllTabsInOneBusProcessFunction extends DTOSourceTagProcessFunction {

        public MergeAllTabsInOneBusProcessFunction(DataXName dataXName, Map<String, OutputTag<DTO>> tab2OutputTag) {
            super(dataXName, tab2OutputTag);
        }

        @Override
        protected String getTableName(DTO record) {
            return KEY_MERGE_ALL_TABS_IN_ONE_BUS;
        }
    }
}

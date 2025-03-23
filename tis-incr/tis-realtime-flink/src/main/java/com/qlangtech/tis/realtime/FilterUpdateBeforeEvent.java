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

import com.google.common.collect.Sets;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.metrics.Counter;

import java.util.List;
import java.util.Set;

import static com.qlangtech.tis.realtime.BasicTISSinkFactory.KEY_SKIP_UPDATE_BEFORE_EVENT;

/**
 * 由于Sink端支持upset更新方式，需要将source中update_before事件类型去除掉
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-19 10:09
 **/
public class FilterUpdateBeforeEvent {

    private static abstract class BasicFilter<T> extends RichFilterFunction<T> {

        private final Set<EventType> filterRowKinds;
        private final boolean shallFilterRowKinds;
        private final boolean supportUpset;
        private transient Counter filteredRecordsCounter;

        public BasicFilter(boolean supportUpset, List<EventType> filterRowKinds) {
            this.filterRowKinds = Sets.newHashSet(filterRowKinds);
            this.shallFilterRowKinds = CollectionUtils.isNotEmpty(filterRowKinds);
            this.supportUpset = supportUpset;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册或获取名为 "filteredRecords" 的计数器
            this.filteredRecordsCounter = getRuntimeContext()
                    .getMetricGroup()
                    .counter(KEY_SKIP_UPDATE_BEFORE_EVENT + "Count");
        }

        protected abstract DTO.EventType getRowKind(T row);

        @Override
        public final boolean filter(T dto) throws Exception {
            DTO.EventType eventType = getRowKind(dto);
            if (this.supportUpset && (eventType == EventType.UPDATE_BEFORE)) {
                this.filteredRecordsCounter.inc();
                return false;
            }

            if (this.shallFilterRowKinds && filterRowKinds.contains(eventType)) {
                this.filteredRecordsCounter.inc();
                return false;
            }

            return true;
        }
    }


    public static class DTOFilter extends BasicFilter<DTO> {
        public DTOFilter(boolean supportUpset, List<EventType> filterRowKinds) {
            super(supportUpset, filterRowKinds);
        }

        @Override
        protected DTO.EventType getRowKind(DTO row) {
            return row.getEventType();
        }

    }

    public static class RowDataFilter extends BasicFilter<RowData> {


        public RowDataFilter(boolean supportUpset, List<EventType> filterRowKinds) {
            super(supportUpset, filterRowKinds);
        }

        @Override
        protected EventType getRowKind(RowData row) {
            switch (row.getRowKind()) {
                case UPDATE_AFTER:
                    return EventType.UPDATE_AFTER;
                case DELETE:
                    return EventType.DELETE;
                case INSERT:
                    return EventType.ADD;
                case UPDATE_BEFORE:
                    return EventType.UPDATE_BEFORE;
                default:
                    throw new IllegalStateException("illegal event type:" + row.getRowKind());
            }
        }
    }


}

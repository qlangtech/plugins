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

package com.qlangtech.plugins.incr.flink.metrics;

import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.realtime.yarn.rpc.MasterJob;
import com.qlangtech.tis.realtime.yarn.rpc.UpdateCounterMap;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.util.Map;
import java.util.Optional;

/**
 * https://github.com/datavane/tis/issues/397
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 10:24
 **/
public class TISPBReporter extends AbstractReporter implements Scheduled {
    @Override
    public String filterCharacters(String s) {
        return CharacterFilter.NO_OP_FILTER.filterCharacters(s);
    }

    private RpcServiceReference rpcService;

    @Override
    public void open(MetricConfig metricConfig) {

        try {
            ITISCoordinator coordinator = ITISCoordinator.create(true, Optional.empty());
            this.rpcService = StatusRpcClientFactory.getService(coordinator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
//<jobmanager>
//  <job.JobName.jobID>
//    <task.TaskName>
//      <operator.OperatorName>
//        <subtask_index>

        String[] scope = group.getScopeComponents();
// 输出示例：["jobmanager", "job_MyJob", "task_Source", "operator_Map", "0"]
        super.notifyOfAddedMetric(metric, metricName, group);
    }

    @Override
    public void close() {

    }

    @Override
    public void report() {
        UpdateCounterMap upateCounter = null;

        UpdateCounterMap updateCounterMap = new UpdateCounterMap();
//        updateCounterMap.setGcCounter(BasicONSListener.getGarbageCollectionCount());
//        updateCounterMap.setFrom(hostName);
//        long currentTimeInSec = ConsumeDataKeeper.getCurrentTimeInSec();
//        updateCounterMap.setUpdateTime(currentTimeInSec);
        // 汇总一个节点中所有索引的增量信息
//        for (IOnsListenerStatus l : incrChannels) {
//            TableSingleDataIndexStatus tableUpdateCounter = new TableSingleDataIndexStatus();
//            tableUpdateCounter.setBufferQueueRemainingCapacity(l.getBufferQueueRemainingCapacity());
//            tableUpdateCounter.setBufferQueueUsedSize(l.getBufferQueueUsedSize());
//            tableUpdateCounter.setConsumeErrorCount((int) l.getConsumeErrorCount());
//            tableUpdateCounter.setIgnoreRowsCount((int) l.getIgnoreRowsCount());
//            tableUpdateCounter.setUUID(this.indexUUID.get(l.getCollectionName()));
//            tableUpdateCounter.setTis30sAvgRT(((BasicONSListener) l).getTis30sAvgRT());
//            // 汇总一个索引中所有focus table的增量信息
//            for (Map.Entry<String, IIncreaseCounter> entry : l.getUpdateStatic()) {
//                // IncrCounter tableIncrCounter = new
//                // IncrCounter((int)entry.getValue().getIncreasePastLast());
//                // tableIncrCounter.setAccumulationCount(entry.getValue().getAccumulation());
//                // tableUpdateCounter.put(entry.getKey(), tableIncrCounter);
//                // 只记录一个消费总量和当前时间
//                tableUpdateCounter.put(entry.getKey(), entry.getValue().getAccumulation());
//            }
//            tableUpdateCounter.put(TABLE_CONSUME_COUNT, ((BasicONSListener) l).getTableConsumeCount());
//            updateCounterMap.addTableCounter(l.getCollectionName(), tableUpdateCounter);
//        }

       // MasterJob masterJob = this.rpcService.reportStatus(upateCounter);
        for (Map.Entry<Counter, String> entry : counters.entrySet()) {
            Counter counter = entry.getKey();
            String metricName = entry.getValue();
            System.out.println(metricName + ": " + counter.getCount());
        }

    }
}

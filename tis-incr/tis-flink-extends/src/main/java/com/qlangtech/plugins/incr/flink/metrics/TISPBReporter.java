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

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.realtime.SourceProcessFunction;
import com.qlangtech.tis.realtime.transfer.IIncreaseCounter;
import com.qlangtech.tis.realtime.transfer.TableSingleDataIndexStatus;
import com.qlangtech.tis.realtime.yarn.rpc.MasterJob;
import com.qlangtech.tis.realtime.yarn.rpc.PipelineFlinkTaskId;
import com.qlangtech.tis.realtime.yarn.rpc.UpdateCounterMap;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * https://github.com/datavane/tis/issues/397
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 10:24
 **/
public class TISPBReporter extends AbstractReporter implements Scheduled {
    private final ConcurrentMap<String /**metric identity*/, Pair<String /**metricName*/, MetricGroup>> metricIdentifierMapper = Maps.newConcurrentMap();

    //private static final ConcurrentMap<String /**taskId*/, MasterJob> receivedTaskControllerMessage = Maps.newConcurrentMap();

    @Override
    public String filterCharacters(String s) {
        return CharacterFilter.NO_OP_FILTER.filterCharacters(s);
    }

//    public static MasterJob getReceivedTaskControllerMessage(String flinkTaskId) {
//        if (StringUtils.isEmpty(flinkTaskId)) {
//            throw new IllegalArgumentException("param flinkTaskId can not be empty");
//        }
//        return receivedTaskControllerMessage.get(flinkTaskId);
//    }

    private static RpcServiceReference rpcService;

    @Override
    public void open(MetricConfig metricConfig) {

    }

    private static RpcServiceReference getRpcService() {
        if (rpcService == null) {
            synchronized (TISPBReporter.class) {
                if (rpcService == null) {
                    try {
                        ITISCoordinator coordinator = ITISCoordinator.create(true, Optional.empty());
                        rpcService = StatusRpcClientFactory.getService(coordinator);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

        }
        return rpcService;
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
        String name = group.getMetricIdentifier(metricName, this);
        if (scope.length > 1 && TaskExecutor.TASK_MANAGER_NAME.equals(scope[1])) {
            if (IIncreaseCounter.TABLE_CONSUME_COUNT.equals(metricName)) {
                //  System.out.println(metricName);
                metricIdentifierMapper.put(name, Pair.of(metricName, group));
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        super.notifyOfRemovedMetric(metric, metricName, group);
        String name = group.getMetricIdentifier(metricName, this);
        metricIdentifierMapper.remove(name);
    }

    @Override
    public void close() {
        getRpcService().close();
    }

    @Override
    public void report() {
//        UpdateCounterMap upateCounter = null;
//
//        UpdateCounterMap updateCounterMap = new UpdateCounterMap();
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
        Pair<String /**metricName*/, MetricGroup> metricGroup = null;
        // MasterJob masterJob = this.rpcService.reportStatus(upateCounter);
        List<UseableMetricForTIS> metrics = Lists.newArrayList();
        for (Map.Entry<Counter, String> entry : counters.entrySet()) {
            Counter counter = entry.getKey();
            String metricID = entry.getValue();
            // System.out.println(metricID + ": " + counter.getCount());
            metricGroup = metricIdentifierMapper.get(metricID);
            if (metricGroup != null) {
               // System.out.println(metricGroup);

                metrics.add(new UseableMetricForTIS(counter, /**metricName*/metricGroup.getKey(), metricGroup.getRight()));
            }
        }

        this.sendMetric2TISAssemble(metrics);
    }

    private void sendMetric2TISAssemble(List<UseableMetricForTIS> metrics) {
        // 汇总一个索引中所有focus table的增量信息
        Map<PipelineFlinkTaskId, TableSingleDataIndexStatus> tabCounterMapper = Maps.newHashMap();

        PipelineFlinkTaskId pipelineName = null;
        TableSingleDataIndexStatus singleDataIndexStatus = null;
        String host = null;
        for (UseableMetricForTIS metric : metrics) {
            pipelineName = new PipelineFlinkTaskId(metric.getPipelineName(), metric.getFlinkTaskId());
            if (host == null) {
                host = metric.getHost();
            }
            singleDataIndexStatus = tabCounterMapper.get(pipelineName);
            if (singleDataIndexStatus == null) {
                singleDataIndexStatus = new TableSingleDataIndexStatus();
                singleDataIndexStatus.setUUID(metric.getFlinkTaskId());
                tabCounterMapper.put(pipelineName, singleDataIndexStatus);
            }
            singleDataIndexStatus.put(metric.metricName, metric.counter.getCount());
        }

        if (MapUtils.isNotEmpty(tabCounterMapper)) {
            UpdateCounterMap pipelineUpdateCounterMap = new UpdateCounterMap();
            if (StringUtils.isEmpty(host)) {
                throw new IllegalStateException("host can not be empty");
            }
            pipelineUpdateCounterMap.setFrom(host);
            tabCounterMapper.forEach((tisPipeline, tabCounter) -> {

                pipelineUpdateCounterMap.setPipelineTableCounterMetric(tisPipeline, tabCounter);
            });
            /**
             * 服务端：IncrStatusUmbilicalProtocolImpl
             */
            getRpcService().reportStatus(pipelineUpdateCounterMap);
        }
    }

    private static class UseableMetricForTIS {
        private Counter counter;
        private String metricName;
        private MetricGroup metricGroup;

        String getPipelineName() {
            return metricGroup.getAllVariables().get(ScopeFormat.SCOPE_JOB_NAME);
        }

        String getHost() {
            return metricGroup.getAllVariables().get(ScopeFormat.SCOPE_HOST);
        }

        String getFlinkTaskId() {
            return metricGroup.getAllVariables().get(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX);
        }

        public UseableMetricForTIS(Counter counter, String metricName, MetricGroup metricGroup) {
            this.counter = counter;
            this.metricName = metricName;
            this.metricGroup = metricGroup;
        }
    }
}

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
import com.qlangtech.tis.realtime.transfer.IIncreaseCounter;
import com.qlangtech.tis.realtime.transfer.ListenerStatusKeeper.LimitRateTypeAndRatePerSecNums;
import com.qlangtech.tis.realtime.transfer.TableSingleDataIndexStatus;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO.RateControllerType;
import com.qlangtech.tis.realtime.yarn.rpc.PipelineFlinkTaskId;
import com.qlangtech.tis.realtime.yarn.rpc.UpdateCounterMap;

import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricType;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import static com.qlangtech.tis.realtime.transfer.IIncreaseCounter.METRIC_LIMIT_RATE_CONTROLLER_TYPE;
import static com.qlangtech.tis.realtime.transfer.IIncreaseCounter.METRIC_LIMIT_RATE_PER_SECOND_NUMS;

/**
 * https://github.com/datavane/tis/issues/397
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 10:24
 **/
public class TISPBReporter extends AbstractReporter implements Scheduled {

    private final ConcurrentMap<String /**metric identity*/, Pair<String /**metricName*/, MetricGroup>>
            counterMetricIdentifierMapper = Maps.newConcurrentMap();

    private final ConcurrentMap<String /**metric identity*/, Pair<String /**metricName*/, MetricGroup>>
            gaugeMetricIdentifierMapper = Maps.newConcurrentMap();

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
            if (IIncreaseCounter.COLLECTABLE_TABLE_COUNT_METRIC.contains(metricName)) {
                //  System.out.println(metricName);
                counterMetricIdentifierMapper.put(name, Pair.of(metricName, group));
            } else if (IIncreaseCounter.COLLECTABLE_METRIC_LIMIT_GAUGE.contains(metricName)) {
                gaugeMetricIdentifierMapper.put(name, Pair.of(metricName, group));
            }

        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        super.notifyOfRemovedMetric(metric, metricName, group);
        String name = group.getMetricIdentifier(metricName, this);
        if (metric.getMetricType() == MetricType.COUNTER) {
            counterMetricIdentifierMapper.remove(name);
        } else if (metric.getMetricType() == MetricType.GAUGE) {
            gaugeMetricIdentifierMapper.remove(name);
        }

    }

    @Override
    public void close() {
        getRpcService().close();
    }

    @Override
    public void report() {
        Pair<String /**metricName*/, MetricGroup> metricGroup = null;
        // MasterJob masterJob = this.rpcService.reportStatus(upateCounter);
        List<UseableMetricForTIS> metrics = Lists.newArrayList();
        String metricID = null;
        for (Map.Entry<Counter, String> entry : counters.entrySet()) {
            Counter counter = entry.getKey();
            metricID = entry.getValue();
            // System.out.println(metricID + ": " + counter.getCount());
            metricGroup = counterMetricIdentifierMapper.get(metricID);
            if (metricGroup != null) {
                metrics.add(new UseableCounterMetricForTIS(counter, /**metricName*/metricGroup.getKey(), metricGroup.getRight()));
            }
        }

        for (Map.Entry<Gauge<?>, String> entry : this.gauges.entrySet()) {
            Gauge<?> gauge = entry.getKey();
            metricID = entry.getValue();
            metricGroup = gaugeMetricIdentifierMapper.get(metricID);
            if (metricGroup != null) {
                metrics.add(new UseableGaugeMetricForTIS(gauge, /**metricName*/metricGroup.getKey(), metricGroup.getRight()));
            }
        }

        this.sendMetric2TISAssemble(metrics);
    }

    private void sendMetric2TISAssemble(List<UseableMetricForTIS> metrics) {
        // 汇总一个索引中所有focus table的增量信息
        Map<PipelineFlinkTaskId, TableSingleDataIndexStatus> tabCounterMapper = Maps.newHashMap();
        UpdateCounterMap pipelineUpdateCounterMap = new UpdateCounterMap();
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
            metric.putCounterMetric(singleDataIndexStatus);
            //  metric.putGaugeMetric(pipelineUpdateCounterMap);
        }

        if (MapUtils.isNotEmpty(tabCounterMapper)) {

            if (StringUtils.isEmpty(host)) {
                throw new IllegalStateException("host can not be empty");
            }
            // pipelineUpdateCounterMap.setGcCounter();


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

    private static abstract class UseableMetricForTIS<METRIC_TYPE> {
        protected METRIC_TYPE metric;
        protected String metricName;
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


        protected abstract <VAL> VAL getMetricVal();

        public UseableMetricForTIS(METRIC_TYPE counter, String metricName, MetricGroup metricGroup) {
            this.metric = counter;
            this.metricName = metricName;
            this.metricGroup = metricGroup;
        }


        public abstract void putCounterMetric(TableSingleDataIndexStatus singleDataIndexStatus);

        @Override
        public String toString() {
            return "metricName='" + metricName + '\'' +
                    ", metricVal=" + this.getMetricVal();
        }
// public abstract void putGaugeMetric(UpdateCounterMap pipelineUpdateCounterMap);
    }

    private static class UseableCounterMetricForTIS extends UseableMetricForTIS<Counter> {
        public UseableCounterMetricForTIS(Counter counter, String metricName, MetricGroup metricGroup) {
            super(counter, metricName, metricGroup);
        }

        @Override
        protected Long getMetricVal() {
            return this.metric.getCount();
        }

        @Override
        public void putCounterMetric(TableSingleDataIndexStatus singleDataIndexStatus) {
            singleDataIndexStatus.put(this.metricName, this.getMetricVal());
        }
    }

    private static class UseableGaugeMetricForTIS extends UseableMetricForTIS<Gauge<?>> {
        public UseableGaugeMetricForTIS(Gauge<?> gauge, String metricName, MetricGroup metricGroup) {
            super(gauge, metricName, metricGroup);
        }

        @Override
        protected <VAL> VAL getMetricVal() {
            return (VAL) this.metric.getValue();
        }

        @Override
        public void putCounterMetric(TableSingleDataIndexStatus singleDataIndexStatus) {
            LimitRateTypeAndRatePerSecNums origin = singleDataIndexStatus.getIncrRateLimitConfig();
            LimitRateTypeAndRatePerSecNums config = null;

            if (METRIC_LIMIT_RATE_CONTROLLER_TYPE.equals(this.metricName)) {
                config = new LimitRateTypeAndRatePerSecNums(RateControllerType.parse((short) getMetricVal()), origin.getPerSecRateNums());
            } else if (METRIC_LIMIT_RATE_PER_SECOND_NUMS.equals(this.metricName)) {
                config = new LimitRateTypeAndRatePerSecNums(origin.getControllerType().orElse(null), (int) getMetricVal());
            } else {
                throw new IllegalStateException("illegal metricName:" + this.metricName);
            }

            singleDataIndexStatus.setIncrRateLimitConfig(config);
        }


    }
}

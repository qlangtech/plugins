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
import com.qlangtech.tis.plugin.incr.TISRateLimiter.IResettableRateLimiter;
import com.qlangtech.tis.realtime.transfer.IIncreaseCounter;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO.RateControllerType;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO.RateControllerType.SkipProcess;

/**
 * 负责给主数据流打标，提供给后续流程 {@link com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream} 实现分流
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-27 10:38
 **/
public abstract class SourceProcessFunction<RECORD_TYPE> extends BroadcastProcessFunction<RECORD_TYPE, IncrRateControllerCfgDTO, RECORD_TYPE> {

    //  public static final String KEY_TIS_IO_NUM_RECORDS_IN = "tis_" + MetricNames.IO_NUM_RECORDS_IN;
    private final Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag;
    //    protected AtomicBoolean pauseFlag = new AtomicBoolean(false);
    private static final Logger logger = LoggerFactory.getLogger(SourceProcessFunction.class);
    private final DataXName dataXName;
    /**
     * 包括所有的数据流量（增删改）
     */
    private transient Counter tisNumRecordsIn;
    protected transient Counter tisInsertNumRecordsIn;
    protected transient Counter tisUpdateNumRecordsIn;
    protected transient Counter tisDeleteNumRecordsIn;
    private final AtomicBoolean drain = new AtomicBoolean(false);

    private short limitRateControllerType = -1;
    private int rateRecordsPerSecondGauge;
    private int parallelNum;
    // private String taskId;

    // private transient RateLimiter rateLimiter;
    //  private transient BroadcastState<String, Double> broadcastState;
    //  private final MapStateDescriptor<String, Double> broadcastStateDesc;

    public SourceProcessFunction(DataXName dataXName, Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag) {
        this.tab2OutputTag = tab2OutputTag;
        this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        this.parallelNum = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        // this.taskId = getRuntimeContext().getTaskInfo().getAllocationIDAsString();
        this.tisNumRecordsIn = getRuntimeContext()
                .getMetricGroup()
                .counter(IIncreaseCounter.TABLE_CONSUME_COUNT);

        this.tisInsertNumRecordsIn = getRuntimeContext()
                .getMetricGroup()
                .counter(IIncreaseCounter.TABLE_INSERT_COUNT);

        this.tisUpdateNumRecordsIn = getRuntimeContext()
                .getMetricGroup()
                .counter(IIncreaseCounter.TABLE_UPDATE_COUNT);

        this.tisDeleteNumRecordsIn = getRuntimeContext()
                .getMetricGroup()
                .counter(IIncreaseCounter.TABLE_DELETE_COUNT);

        getRuntimeContext()
                .getMetricGroup().gauge(IIncreaseCounter.METRIC_LIMIT_RATE_CONTROLLER_TYPE, new LimitRateControllerTypeGauge());

        getRuntimeContext()
                .getMetricGroup().gauge(IIncreaseCounter.METRIC_LIMIT_RATE_PER_SECOND_NUMS, new RateRecordsPerSecondGauge());


        // 获取广播状态
        //   ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("rate", Double.class);
        // broadcastState = getRuntimeContext().getBroadcastState(descriptor);
    }

    private class LimitRateControllerTypeGauge implements Gauge<Short> {
        @Override
        public Short getValue() {
            return limitRateControllerType;
        }
    }

    private class RateRecordsPerSecondGauge implements Gauge<Integer> {
        @Override
        public Integer getValue() {
            return rateRecordsPerSecondGauge;
        }
    }


    @Override
    public void processBroadcastElement(IncrRateControllerCfgDTO rate, Context context, Collector<RECORD_TYPE> collector) throws Exception {
//        BroadcastState<String, Double> broadcastState = context.getBroadcastState(broadcastStateDesc);
//        broadcastState.put("rate", rate);
//        rateLimiter.setRate(newRate);

        RateControllerType controllerType
                = Objects.requireNonNull(rate.getControllerType(), "controller type can not be null");
        if (controllerType == SkipProcess) {
            return;
        }
        IResettableRateLimiter rateLimiter = getRateLimiter();
//        if (rate.getPause()) {
//            rateLimiter.pauseConsume();
//        } else {
//            rateLimiter.resumeConsume();
//        }

        Map<String, Object> params = rate.getPayloadParams();
        drain.set(false);
        this.limitRateControllerType = controllerType.getTypeToken();
        switch (controllerType) {
            case Paused: {
                rateLimiter.pauseConsume();
                break;
            }
            case RateLimit: {
                rateLimiter.resumeConsume();
                Integer rateRecordsPerSecond = (Integer) params.get(IncrRateControllerCfgDTO.KEY_RATE_LIMIT_PER_SECOND);
                this.rateRecordsPerSecondGauge = rateRecordsPerSecond * parallelNum;
                resetLimitRate(Objects.requireNonNull(rateRecordsPerSecond, "param rateRecordsPerSecond can not be null"));
                logger.info("set rateRecordsPerSecond:{},rateRecordsPerSecondGauge:{}", rateRecordsPerSecond, this.rateRecordsPerSecondGauge);
                break;
            }
            case NoLimitParam: {
                rateLimiter.resumeConsume();
                resetLimitRate(Integer.MAX_VALUE);
                logger.info("set NoLimitParam");
                break;
            }
            case FloodDischargeRate: {
                rateLimiter.resumeConsume();
                resetLimitRate(Integer.MAX_VALUE);
                drain.set(true);
                logger.info("set FloodDischargeRate");
                break;
            }
            default:
                throw new IllegalStateException("pipeline:" + this.dataXName.getPipelineName() + ",illegal controllerType:" + controllerType);
        }

    }

    private void resetLimitRate(Integer rateRecordsPerSecond) {
        IResettableRateLimiter currentRate = this.getRateLimiter();
        currentRate.resetRate(rateRecordsPerSecond);
    }

    private IResettableRateLimiter getRateLimiter() {
        RateLimiter currentRate = RateLimiterRegistry.getCurrentRate(this.dataXName.getPipelineName());
        if (currentRate == null) {
            throw new NullPointerException("pipe:" + this.dataXName.getPipelineName() + ",relevant currentRate can not be null");
        }
        if (!(currentRate instanceof IResettableRateLimiter)) {
            throw new IllegalStateException("currentRate must be type of "
                    + IResettableRateLimiter.class.getSimpleName() + " but now is " + currentRate.getClass().getClass());
        }
        return (IResettableRateLimiter) currentRate;
    }

//    public void shallPause() throws Exception {
//        if (pauseFlag.get()) {
//            synchronized (pauseFlag) {
//                if (pauseFlag.get()) {
//                    pauseFlag.wait();
//                }
//            }
//        }
//    }

    // public void processElement(RECORD_TYPE recordType, BroadcastProcessFunction<RECORD_TYPE, Double, RECORD_TYPE>.ReadOnlyContext readOnlyContext, Collector<RECORD_TYPE> collector) throws Exception {

    /**
     * 在主流中为每个表打标签
     *
     * @param in   The input value.
     * @param ctx  A {@link Context} that allows querying the timestamp of the element and getting a
     *             {@link org.apache.flink.streaming.api.TimerService} for registering timers and querying the time. The context is only
     *             valid during the invocation of this method, do not store it.
     * @param _out The collector for returning result values.
     * @throws Exception
     * @see com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream#addStream collected in it
     */
    @Override
    public void processElement(RECORD_TYPE in, ReadOnlyContext ctx, Collector<RECORD_TYPE> _out) throws Exception {

        if (drain.get()) {
            // 泄洪
            // increaseNumRecordsMetric(in);
            tisNumRecordsIn.inc();
            return;
        }
        //  shallPause();
        //  ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(broadcastStateDesc);


        //side_output: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/side_output.html
        final String tabName = getTableName(in);// in.getTableName();
        OutputTag<RECORD_TYPE> outputTag = tab2OutputTag.get(tabName);
        if (outputTag == null) {
            throw new IllegalStateException("target table:" + tabName + " can not find relevant in tab2OutputTag");
        }
        ctx.output(outputTag, in);
        increaseNumRecordsMetric(in);
    }

    protected void increaseNumRecordsMetric(RECORD_TYPE in) {
        tisNumRecordsIn.inc();
    }

    protected abstract String getTableName(RECORD_TYPE record);


//    /**
//     * 停止增量接收消息
//     */
//    private void pauseConsume() {
//        if (this.pauseFlag.compareAndSet(false, true)) {
//            logger.info(this.dataXName.getPipelineName() + " get pause command");
//        }
//    }

//    /**
//     * 重新啟動增量消息
//     */
//    public void resumeConsume() {
//        synchronized (this.pauseFlag) {
//            if (this.pauseFlag.compareAndSet(true, false)) {
//                this.pauseFlag.notifyAll();
//                logger.info(this.dataXName.getPipelineName() + " get resume command");
//            }
//        }
//    }
}

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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols;
import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols.OffsetColVal;
import com.alibaba.datax.common.element.QueryCriteria;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSingleProcessorException;
import com.qlangtech.tis.datax.DataXJobSingleProcessorExecutor;
import com.qlangtech.tis.datax.DataXJobSubmit.InstanceType;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataXTaskRelevant;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.preview.IPreviewRowsDataService;
import com.qlangtech.tis.datax.preview.PreviewHeaderCol;
import com.qlangtech.tis.datax.preview.PreviewRowsData;
import com.qlangtech.tis.plugin.datax.DataXPipelinePreviewProcessorExecutor.PreviewLaunchParam;
import com.qlangtech.tis.rpc.grpc.datax.preview.DataXRecordsPreviewGrpc;
import com.qlangtech.tis.rpc.grpc.datax.preview.DataXRecordsPreviewGrpc.DataXRecordsPreviewBlockingStub;
import com.qlangtech.tis.rpc.grpc.datax.preview.HeaderColGrpc;
import com.qlangtech.tis.rpc.grpc.datax.preview.OffsetColValGrpc;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataCriteria;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataCriteria.Builder;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataResponse;
import com.qlangtech.tis.rpc.grpc.datax.preview.StringValue;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 启动新的基于独立进程的数据预览执行启动器
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-23 21:18
 * @see com.qlangtech.tis.datax.DataxSplitTabSyncConsumer
 **/
public class DataXPipelinePreviewProcessorExecutor extends DataXJobSingleProcessorExecutor<PreviewLaunchParam> implements IPreviewRowsDataService {
    private static final Logger logger = LoggerFactory.getLogger(DataXPipelinePreviewProcessorExecutor.class);
    private String classpath;
    private final int newGrpcPort;
    private PreviewProgressorExpireTracker commitTracker;


    public DataXPipelinePreviewProcessorExecutor(int newGrpcPort) {
        this.newGrpcPort = newGrpcPort;
    }

    //  useRuntimePropEnvProps
    @Override
    protected boolean useRuntimePropEnvProps() {
        return false;
    }

    private DataXRecordsPreviewBlockingStub blockingStub;


    /**
     * 客户端调用，发送到同虚拟机的grpc 服务端进行访问
     *
     * @param dataXName
     * @param tableName
     * @param queryCriteria
     * @return
     */
    @Override
    public PreviewRowsData previewRowsData(
            String dataXName, String tableName, QueryCriteria queryCriteria) {

        if (blockingStub == null) {
            try {
                synchronized (this) {
                    if (blockingStub == null) {
                        final ManagedChannel channel
                                = ManagedChannelBuilder.forTarget("127.0.0.1:" + newGrpcPort)
                                .usePlaintext()//.enableRetry().maxRetryAttempts(5)
                                .build();

                        CountDownLatch countDown = new CountDownLatch(1);
                        channel.notifyWhenStateChanged(ConnectivityState.READY, () -> {
                            countDown.countDown();
                        });
                        this.blockingStub = DataXRecordsPreviewGrpc.newBlockingStub(channel);
                        if (!countDown.await(20, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(" can not connection to preview grpc service ");
                        }
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // 每次操作都要标记一下
        Objects.requireNonNull(this.commitTracker, "commitTracker can not be null").scheduleCommitWithin(-1);
        Builder rowsDataBuilder = PreviewRowsDataCriteria.newBuilder();

        rowsDataBuilder.setDataXName(dataXName);
        rowsDataBuilder.setTableName(tableName);
        rowsDataBuilder.setPageSize(queryCriteria.getPageSize());
        rowsDataBuilder.setNext(queryCriteria.isNextPakge());
        List<OffsetColVal> orderByCols = null;
        if (CollectionUtils.isNotEmpty(orderByCols = queryCriteria.getPagerOffsetCursor())) {
//            if (CollectionUtils.isEmpty(orderByCols.getOffsetCols())) {
//                throw new IllegalStateException("dataXName:" + dataXName
//                        + ",table:" + tableName + " relevant offsetCols can not be empty");
//            }
//            PreviewOrderByCols.Builder orderByColsBuilder = PreviewOrderByCols.newBuilder();
//            orderByColsBuilder.addAllOffsetCols(orderByCols.getOffsetCols().stream().map((col) -> {
//                OffsetColValGrpc.Builder gcol = OffsetColValGrpc.newBuilder();
//                gcol.setColKey(col.getColKey());
//                gcol.setVal(col.getVal());
//                gcol.setIsNumericJdbcType(col.isNumericJdbcType());
//                return gcol.build();
//            }).collect(Collectors.toList()));
//
//            rowsDataBuilder.setOrderByCols(orderByColsBuilder.build());
            rowsDataBuilder.addAllOrderByCols(orderByCols.stream().map((col) -> {
                OffsetColValGrpc.Builder colValBuilder = OffsetColValGrpc.newBuilder();
                colValBuilder.setNumericJdbcType(col.isNumericJdbcType());
                colValBuilder.setColKey(col.getColKey());
                colValBuilder.setVal(col.getVal());
                return colValBuilder.build();
            }).collect(Collectors.toList()));
        }


        PreviewHeaderCol[] header = null;
        List<String[]> rows = null;
        String[] cols = null;
        int retryCount = 0;
        aa:
        while (true) {

            try {
                PreviewRowsDataResponse response = blockingStub.previewRowsData(rowsDataBuilder.build());

                List<DataXResultPreviewOrderByCols.OffsetColVal> headerCursor = Lists.newArrayList();
                List<DataXResultPreviewOrderByCols.OffsetColVal> tailerCursor = Lists.newArrayList();
                //  DataXResultPreviewOrderByCols.OffsetColVal colVal = null;
                for (OffsetColValGrpc offsetColVal : response.getHeaderCursorList()) {
                    headerCursor.add(new OffsetColVal(offsetColVal.getColKey(), offsetColVal.getVal(), offsetColVal.getNumericJdbcType()));
                }

                for (OffsetColValGrpc offsetColVal : response.getTailerCursorList()) {
                    tailerCursor.add(new OffsetColVal(offsetColVal.getColKey(), offsetColVal.getVal(), offsetColVal.getNumericJdbcType()));
                }

                Map<String, HeaderColGrpc> columnHeader = response.getColumnHeaderMap();
                List<com.qlangtech.tis.rpc.grpc.datax.preview.Record> records = response.getRecordsList();

                header = new PreviewHeaderCol[columnHeader.size()];
                rows = Lists.newArrayListWithCapacity(records.size());
                PreviewHeaderCol headerCol = null;
                HeaderColGrpc col = null;
                for (Map.Entry<String, HeaderColGrpc> entry : columnHeader.entrySet()) {
                    headerCol = new PreviewHeaderCol(entry.getKey());
                    col = entry.getValue();
                    headerCol.setBlob(col.getBlob());
                    header[col.getIndex()] = headerCol;
                }
                java.util.List<StringValue> vals = null;


                for (com.qlangtech.tis.rpc.grpc.datax.preview.Record record : records) {
                    vals = record.getColValsList();
                    rows.add(vals.stream().map((val) -> val.getNil() ? null : val.getVal()).toArray(String[]::new));
                }

                return new PreviewRowsData(header, rows, headerCursor, tailerCursor);

            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.UNAVAILABLE && retryCount++ < 3) {
                    logger.warn(e.getMessage());
                    // Handle temporary unavailability
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                    continue aa;
                } else {

                    throw e; // Re-throw non-retriable exceptions
                }
            }
        }

    }

    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }


    @Override
    public void consumeMessage(PreviewLaunchParam msg) throws Exception {
        super.consumeMessage(msg);
    }

    @Override
    protected void waitForTerminator(Integer jobId, String dataxName, DefaultExecuteResultHandler resultHandler) throws InterruptedException, DataXJobSingleProcessorException {
        // super.waitForTerminator(jobId, dataxName, resultHandler);
    }

    @Override
    protected void addMainClassParams(PreviewLaunchParam msg, Integer taskId, String jobName, String dataxName, CommandLine cmdLine) {
        cmdLine.addArgument(dataxName);
        cmdLine.addArgument(String.valueOf(this.newGrpcPort));
    }

    @Override
    protected InstanceType getExecMode() {
        return InstanceType.LOCAL;
    }

    @Override
    protected String getClasspath() {
        return this.classpath;
    }

    @Override
    protected String getMainClassName() {
        return DataXPipelinePreviewMain.class.getName();
    }

    @Override
    protected File getWorkingDirectory() {
        return DataXJobInfo.getDataXExecutorDir();
    }

    @Override
    protected String getIncrStateCollectAddress() {
        return "";
    }

    public void setCommitTracker(PreviewProgressorExpireTracker commitTracker) {
        this.commitTracker = Objects.requireNonNull(commitTracker, "commitTracker can not be null");
    }

    public static class PreviewLaunchParam implements IDataXTaskRelevant {
        private static final int DEFAULT_TASK_ID = 999;
        private final String dataXName;

        public PreviewLaunchParam(String dataXName) {
            this.dataXName = dataXName;
        }

        @Override
        public Integer getTaskId() {
            return DEFAULT_TASK_ID;
        }

        @Override
        public String getJobName() {
            return "";
        }

        @Override
        public String getDataXName() {
            return this.dataXName;
        }

        @Override
        public long getExecEpochMilli() {
            return 0;
        }

        @Override
        public <T> void setAttr(Class<T> key, Object val) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T getAttr(Class<T> key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTaskSerializeNum() {
            return 0;
        }

        @Override
        public String getFormatTime(TimeFormat format) {
            return "";
        }
    }
}

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

package com.qlangtech.tis.plugin.datax.grpc;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols.OffsetColVal;
import com.alibaba.datax.common.element.ICol2Index;
import com.alibaba.datax.common.element.ICol2Index.Col;
import com.alibaba.datax.common.element.PreviewRecords;
import com.alibaba.datax.common.element.QueryCriteria;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.TISJarLoader;
import com.qlangtech.tis.datax.common.DataXRealExecutor;
import com.qlangtech.tis.datax.common.WriterPluginMeta;
import com.qlangtech.tis.rpc.grpc.datax.preview.DataXRecordsPreviewGrpc.DataXRecordsPreviewImplBase;
import com.qlangtech.tis.rpc.grpc.datax.preview.HeaderColGrpc;
import com.qlangtech.tis.rpc.grpc.datax.preview.OffsetColValGrpc;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataCriteria;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataResponse;
import com.qlangtech.tis.rpc.grpc.datax.preview.PreviewRowsDataResponse.Builder;
import com.qlangtech.tis.rpc.grpc.datax.preview.Record;
import com.qlangtech.tis.rpc.grpc.datax.preview.StringValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-24 14:39
 **/
public class DefaultDataXPreviewRocrdsImpl extends DataXRecordsPreviewImplBase {
    private final DataXRealExecutor pipeSynchronize;
    private static final Logger logger = LoggerFactory.getLogger(DefaultDataXPreviewRocrdsImpl.class);

    public static void main(String[] args) throws Exception {
        String dataXName = args[0];
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("param dataXName can not be empty");
        }

        DefaultDataXPreviewRocrdsImpl previewRocrds
                = DefaultDataXPreviewRocrdsImpl.create(dataXName);

        int pageSize = 15;
        boolean next = true;
        QueryCriteria queryCriteria = new QueryCriteria();
        queryCriteria.setPageSize(pageSize);
        queryCriteria.setNextPakge(next);
        //  DataXResultPreviewOrderByCols previewOrderByCols = null;// new DataXResultPreviewOrderByCols();
        // previewOrderByCols.addOffsetColVal();

//        queryCriteria.setPagerOffsetCursor(
//                Collections.singletonList(new OffsetColVal("order_id", "100824153611828318057481343300ae", false)));


        PreviewRecords records
                = previewRocrds.pipeSynchronize.previewRecords("base", queryCriteria);
        records.getPageRows().forEach((r) -> {
           // System.out.println(r.getColumn("order_id"));
            System.out.println(r);
        });
    }

    public static DefaultDataXPreviewRocrdsImpl create(String dataXName) throws Exception {


        Map<String, Object> writerConf = Maps.newHashMap();
        final String wirterPluginName = "datagridwriter";
        writerConf.put("name", wirterPluginName);
        WriterPluginMeta writerPluginMeta //
                = new WriterPluginMeta("plugin.writer." + wirterPluginName
                , "com.qlangtech.tis.plugin.datax.writer.DataGridWriter", Configuration.from(writerConf));

        TISJarLoader uberClassLoader = new TISJarLoader(TIS.get().getPluginManager());

        DataXRealExecutor pripeSynchronize = WriterPluginMeta.realExecute(dataXName
                , writerPluginMeta //
                , Optional.of(uberClassLoader));

        return new DefaultDataXPreviewRocrdsImpl(pripeSynchronize);
    }

    private DefaultDataXPreviewRocrdsImpl(DataXRealExecutor pipeSynchronize) {
        this.pipeSynchronize = Objects.requireNonNull(pipeSynchronize, "pipeSynchronize can not be null");
    }


    private static List<OffsetColValGrpc> offsetColVal(List<OffsetColVal> cursorVals) {
        OffsetColValGrpc.Builder offsetColsBuilder = null;
        List<OffsetColValGrpc> offsetColVal = null;
        if (CollectionUtils.isNotEmpty(cursorVals)) {
            offsetColVal = Lists.newArrayList();
            for (OffsetColVal val : cursorVals) {
                offsetColsBuilder = OffsetColValGrpc.newBuilder();
                offsetColsBuilder.setColKey(val.getColKey());
                offsetColsBuilder.setVal(val.getVal());
                offsetColsBuilder.setNumericJdbcType(val.isNumericJdbcType());
                offsetColVal.add(offsetColsBuilder.build());
            }
            return offsetColVal;
        }
        return null;
    }

    @Override
    public void previewRowsData(PreviewRowsDataCriteria request, StreamObserver<PreviewRowsDataResponse> responseObserver) {

        try {
            String tableName = request.getTableName();
            boolean next = request.getNext();
            final int pageSize = request.getPageSize();

            Builder responseBuilder = PreviewRowsDataResponse.newBuilder();


            QueryCriteria queryCriteria = createQueryCriteria(request, pageSize, next);

            PreviewRecords records = pipeSynchronize.previewRecords(tableName, queryCriteria);
//            List<com.alibaba.datax.common.element.Record> records
//                    = pipeSynchronize.previewRecords(tableName, queryCriteria);

            List<OffsetColValGrpc> offsetColVal = null;
            if (CollectionUtils.isNotEmpty(offsetColVal = offsetColVal(records.getHeaderCursor()))) {
                responseBuilder.addAllHeaderCursor(offsetColVal);
            }

            if (CollectionUtils.isNotEmpty(offsetColVal = offsetColVal(records.getTailerCursor()))) {
                responseBuilder.addAllTailerCursor(offsetColVal);
            }

            Map<String, Col> col2IndexMapper = null;
            HeaderColGrpc.Builder colGrpcBuilder = null;
            for (com.alibaba.datax.common.element.Record row : records.getPageRows()) {
                col2IndexMapper = row.getCol2Index().getCol2Index();
                for (Map.Entry<String, Col> entry : col2IndexMapper.entrySet()) {

                    colGrpcBuilder = HeaderColGrpc.newBuilder();
                    colGrpcBuilder.setBlob(isBlob(entry.getValue()));
                    colGrpcBuilder.setIndex(entry.getValue().getIndex());

                    responseBuilder.putColumnHeader(entry.getKey(), colGrpcBuilder.build());
                }
                break;
            }

            if (col2IndexMapper == null) {
                //  throw new IllegalStateException("col2IndexMapper can not be null,records size:" + records.size());
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            Column col = null;
            StringValue[] colVals = null;
            StringValue.Builder strValBuilder = null;
            for (com.alibaba.datax.common.element.Record record : records.getPageRows()) {
                Record.Builder recordBuilder = Record.newBuilder();
                colVals = new StringValue[col2IndexMapper.size()];
                for (Map.Entry<String, Col> entry : col2IndexMapper.entrySet()) {
                    col = record.getColumn(entry.getValue().getIndex());
                    strValBuilder = StringValue.newBuilder();
                    strValBuilder.setNil(true);
                    if (col != null && col.getRawData() != null) {
                        //  recordBuilder.setColVal(entry.getValue(), String.valueOf(col.getRawData()));
                        strValBuilder.setNil(false);
                        strValBuilder.setVal(String.valueOf(col.getRawData()));
                    }
                    colVals[entry.getValue().getIndex()] = strValBuilder.build();
                }

                recordBuilder.addAllColVals(Arrays.asList(colVals));
                responseBuilder.addRecords(recordBuilder.build());
            }

            //=============================================
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            Status status = Status.UNKNOWN.withCause(e).withDescription(e.getMessage());
//// Throw a StatusException
//            throw status.asRuntimeException();
            responseObserver.onError(status.asException());
            logger.error(e.getMessage(), e);
        }
    }

    private boolean isBlob(ICol2Index.Col col) {
        switch (col.getType().getJdbcType()) {
            case BLOB:
            case LONGVARBINARY:
            case VARBINARY:
            case BINARY: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    private static QueryCriteria createQueryCriteria(PreviewRowsDataCriteria request, int pageSize, boolean next) {
        QueryCriteria queryCriteria = new QueryCriteria();
        queryCriteria.setPageSize(pageSize);
        queryCriteria.setNextPakge(next);
        List<OffsetColValGrpc> orderByCols = request.getOrderByColsList();
        if (CollectionUtils.isNotEmpty(orderByCols)) {
            queryCriteria.setPagerOffsetCursor(orderByCols.stream().map((col) -> {
                return new OffsetColVal(col.getColKey(), col.getVal(), col.getNumericJdbcType());
            }).collect(Collectors.toList()));
        }

//        if (orderByCols != null) {
//            DataXResultPreviewOrderByCols pointCols = new DataXResultPreviewOrderByCols(true);
//            for (com.qlangtech.tis.rpc.grpc.datax.preview.OffsetColValGrpc orderByCol : orderByCols.getOffsetColsList()) {
//                OffsetColVal colVal = new OffsetColVal(orderByCol.getColKey(), orderByCol.getVal(), orderByCol.getIsNumericJdbcType());
//                pointCols.addOffsetColVal(colVal);
//            }
//            queryCriteria.setPagerOffsetPointCols(pointCols);
//        }
        return queryCriteria;
    }
}

///**
// * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
// * <p>
// * This program is free software: you can use, redistribute, and/or modify
// * it under the terms of the GNU Affero General Public License, version 3
// * or later ("AGPL"), as published by the Free Software Foundation.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT
// * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// * FITNESS FOR A PARTICULAR PURPOSE.
// * <p>
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package com.qlangtech.tis.realtime;
//
//import com.qlangtech.tis.realtime.transfer.DTO;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//import java.io.Serializable;
//import java.util.Map;
//
////import org.apache.flink.types.Row;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2021-10-05 12:30
// **/
//public class TISFlinkSourceHandle extends BasicFlinkSourceHandle implements Serializable {
//
//    @Override
//    protected void processTableStream(Map<String, DataStream<DTO>> streamMap, SinkFunction<DTO> sinkFunction) {
//        DataStream<DTO> waitinginstanceinfoStream = streamMap.get("waitinginstanceinfo");
//
////        stream = stream.keyBy((d) -> d.getAfter().get("waitinginstance_id"))
////                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
////                .reduce((r1, r2) -> {
////                    System.out.println("===============================");
////                    return r2;
////                });
//
//
//        waitinginstanceinfoStream.addSink(sinkFunction);
//        waitinginstanceinfoStream.addSink(new PrintSinkFunction());
//    }
//}

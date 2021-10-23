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
// *
// */
//
//package com.qlangtech.tis.realtime
//
//import java.util
//
//import com.qlangtech.tis.realtime.transfer.DTO
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
//
///**
// *
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2021-09-28 16:10
// **/
//class TISFlinkSourceHandle extends BasicFlinkSourceHandle {
//  /**
//   * 处理各个表对应的数据流
//   *
//   * @param
//   */
//  override protected def processTableStream(streamMap: util.Map[String, DataStream[DTO]], sinkFunction: SinkFunction[DTO]): Unit = {
//    // val waitinginstanceinfoStream = streamMap.get("waitinginstanceinfo")
//
//    val waitinginstanceinfoStream = streamMap.get("instancedetail")
//
//    //        stream = stream.keyBy((d) -> d.getAfter().get("waitinginstance_id"))
//    //                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
//    //                .reduce((r1, r2) -> {
//    //                    System.out.println("===============================");
//    //                    return r2;
//    //                });
//
//
//    waitinginstanceinfoStream.addSink(sinkFunction).setParallelism(1).name("elasticserach")
//    waitinginstanceinfoStream.addSink(new PrintSinkFunction[DTO]).name("sinkPrint")
//
//  }
//}

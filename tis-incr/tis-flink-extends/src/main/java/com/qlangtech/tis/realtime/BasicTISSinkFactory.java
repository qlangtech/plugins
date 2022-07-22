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

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-13 23:01
 **/
public abstract class BasicTISSinkFactory<TRANSFER_OBJ> extends TISSinkFactory {

    @Override
    public abstract Map<IDataxProcessor.TableAlias, TabSinkFunc<TRANSFER_OBJ>> createSinkFunction(IDataxProcessor dataxProcessor);

    public final static class DTOSinkFunc extends TabSinkFunc<DTO> {
        public DTOSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<DTO> sinkFunction) {
            super(tab, sinkFunction);
        }

        @Override
        protected DataStream<DTO> streamMap(DataStream<DTO> sourceStream) {
            return sourceStream;
        }
    }

    public static abstract class RowDataSinkFunc extends TabSinkFunc<RowData> {
        public RowDataSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<RowData> sinkFunction) {
            super(tab, sinkFunction);
        }
    }
}

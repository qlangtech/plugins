/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.realtime;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-16 12:32
 **/
public class SinkFuncs {
    private transient final Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFunction;

    public SinkFuncs(Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    public void add2Sink(String originTableName, DataStream<DTO> sourceStream) {

        if (sinkFunction.size() < 2) {
            for (Map.Entry<IDataxProcessor.TableAlias, SinkFunction<DTO>> entry : sinkFunction.entrySet()) {
                sourceStream.addSink(entry.getValue()).name(entry.getKey().getTo());
            }
        } else {
            if (StringUtils.isEmpty(originTableName)) {
                throw new IllegalArgumentException("param originTableName can not be null");
            }
            boolean hasMatch = false;
            for (Map.Entry<IDataxProcessor.TableAlias, SinkFunction<DTO>> entry : sinkFunction.entrySet()) {
                if (originTableName.equals(entry.getKey().getFrom())) {
                    sourceStream.addSink(entry.getValue()).name(entry.getKey().getTo());
                    hasMatch = true;
                    break;
                }
            }
            if (!hasMatch) {
                throw new IllegalStateException("tabName:" + originTableName + " can not find SINK in :"
                        + sinkFunction.keySet().stream()
                        .map((t) -> "(" + t.getFrom() + "," + t.getTo() + ")").collect(Collectors.joining(" ")));
            }
        }
    }
}

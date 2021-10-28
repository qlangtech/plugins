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

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-27 10:19
 **/
public class DTOStream implements Serializable {
    public final OutputTag<DTO> outputTag;
    private transient DataStream<DTO> stream;

    public DTOStream(OutputTag<DTO> outputTag) {
        this.outputTag = outputTag;
    }

    public DataStream<DTO> getStream() {
        return this.stream;
    }

    public void addStream(SingleOutputStreamOperator<DTO> mainStream) {
        if (stream == null) {
            stream = mainStream.getSideOutput(outputTag);
        } else {
            stream = stream.union(mainStream.getSideOutput(outputTag));
        }
    }
}

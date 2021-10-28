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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-27 10:38
 **/
public class SourceProcessFunction extends ProcessFunction<DTO, DTO> {
    private final Map<String, OutputTag<DTO>> tab2OutputTag;

    public SourceProcessFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
        this.tab2OutputTag = tab2OutputTag;
    }

    @Override
    public void processElement(DTO in, Context ctx, Collector<DTO> out) throws Exception {
        //side_output: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/side_output.html
        final String tabName = in.getTableName();// String.valueOf(in.getAfter().get(DTOTypeInfo.KEY_FIELD_TABLE_NAME));
        OutputTag<DTO> outputTag = tab2OutputTag.get(tabName);
        if (outputTag == null) {
            throw new IllegalStateException("target table:" + tabName + " can not find relevant in tab2OutputTag");
        }
        ctx.output(outputTag, in);
    }
}

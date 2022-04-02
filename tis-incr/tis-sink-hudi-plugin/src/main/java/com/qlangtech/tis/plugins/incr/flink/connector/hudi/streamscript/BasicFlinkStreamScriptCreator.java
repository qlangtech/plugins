/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.connector.hudi.streamscript;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.HudiSinkFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-24 10:48
 **/
public class BasicFlinkStreamScriptCreator implements IStreamTableCreator {

    String TEMPLATE_FLINK_HUDI_STREAM_STYLE_HANDLE_SCALA = "flink_hudi_stream_style_handle_scala.vm";

    protected final HudiSinkFactory hudiSinkFactory;

    public BasicFlinkStreamScriptCreator(HudiSinkFactory hudiSinkFactory) {
        this.hudiSinkFactory = hudiSinkFactory;
    }

    public static IStreamTableCreator createStreamTableCreator(HudiSinkFactory hudiSinkFactory) {
//        StreamScriptType scriptType = StreamScriptType.parse();
//        switch (scriptType) {
//            case SQL:
//                return new SQLStyleFlinkStreamScriptCreator(hudiSinkFactory);
//            case STREAM_API:
//                return new StreamAPIStyleFlinkStreamScriptCreator(hudiSinkFactory);
//            default:
//                throw new IllegalStateException("illegal:" + hudiSinkFactory.scriptType);
//        }
        return hudiSinkFactory.scriptType.createStreamTableCreator(hudiSinkFactory);
    }

    @Override
    public final IStreamTableMeta getStreamTableMeta(final String tableName) {
        final Pair<HudiSelectedTab, HudiTableMeta> tabMeta = hudiSinkFactory.getTableMeta(tableName);
        return new IStreamTableMeta() {
            @Override
            public List<HdfsColMeta> getColsMeta() {
                return tabMeta.getRight().colMetas;
            }
        };
    }


}

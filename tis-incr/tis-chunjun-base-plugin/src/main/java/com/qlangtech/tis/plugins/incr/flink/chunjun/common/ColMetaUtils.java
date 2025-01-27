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

package com.qlangtech.tis.plugins.incr.flink.chunjun.common;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.sink.SinkColMetas;
import com.qlangtech.tis.datax.IStreamTableMeataCreator;
import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-26 09:40
 **/
public class ColMetaUtils {

    public static List<IColMetaGetter> getColMetas(
            IStreamTableMeataCreator.ISourceStreamMetaCreator sourceStreamMetaCreator, JdbcConf conf) {
        IStreamTableMeta tabColMetas
                = sourceStreamMetaCreator.getStreamTableMeta(conf.getTable());
        return tabColMetas.getColsMeta().stream().map((c) -> c).collect(Collectors.toList());
    }

    /**
     * 取得目标库的字段类型
     *
     * @param
     * @param conf
     * @return
     */
    public static SinkColMetas getColMetasMap(
            IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaCreator, JdbcConf conf) {
        return getColMetasMap(sinkStreamMetaCreator, conf.getTable());
    }

    public static SinkColMetas getColMetasMap(
            IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaCreator, String tabName) {
        EntityName tab = EntityName.parse(tabName);
        IStreamTableMeta tableMeta = sinkStreamMetaCreator.getStreamTableMeta(tab.getTabName());
        return new SinkColMetas(tableMeta);
    }

}

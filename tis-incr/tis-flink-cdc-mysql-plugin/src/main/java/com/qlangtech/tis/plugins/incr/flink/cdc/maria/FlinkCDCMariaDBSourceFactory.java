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

package com.qlangtech.tis.plugins.incr.flink.cdc.maria;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugins.incr.flink.cdc.mysql.FlinkCDCMySQLSourceFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-14 12:13
 **/
public class FlinkCDCMariaDBSourceFactory extends FlinkCDCMySQLSourceFactory {

    @Override
    public IFlinkColCreator<FlinkCol> createFlinkColCreator(DataSourceMeta sourceMeta) {
        final IFlinkColCreator flinkColCreator = (meta, colIndex) -> {
            return meta.getType().accept(new MariaDBCDCTypeVisitor(meta, colIndex));
        };
        return flinkColCreator;
    }


    @TISExtension()
    public static class MariaDBSourceFactoryDescriptor extends DefaultDescriptor {
        public MariaDBSourceFactoryDescriptor() {
            super();
        }


        @Override
        public EndType getEndType() {
            return EndType.MariaDB;
        }
    }
}

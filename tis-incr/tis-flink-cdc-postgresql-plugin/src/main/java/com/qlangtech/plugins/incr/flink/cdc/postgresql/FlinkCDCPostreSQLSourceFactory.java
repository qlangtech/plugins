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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFactory;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.extension.TISExtension;

/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:15
 **/
@Public
public class FlinkCDCPostreSQLSourceFactory extends FlinkCDCPGLikeSourceFactory {
    @Override
    public IMQListener create() {
        return new FlinkCDCPostgreSQLSourceFunction(this);
    }

//    The name of the Postgres logical decoding plug-in installed on the server. Supported
//         * values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,
//            * wal2json_rds_streaming and pgoutput.


    @TISExtension()
    public static class DefaultDescriptor extends FlinkCDCPGLikeSourceFactory.BasePGLikeDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-PostgreSQL";
        }

        @Override
        public EndType getEndType() {
            return EndType.Postgres;
        }
    }

}

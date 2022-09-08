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

package com.qlangtech.plugins.incr.flink.chunjun.postgresql.source;

import com.qlangtech.plugins.incr.flink.chunjun.source.ChunjunSourceFactory;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.extension.TISExtension;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-27 08:06
 **/
public class ChunjunPostgreSQLSourceFactory extends ChunjunSourceFactory {

    @Override
    public IMQListener create() {
        return new PostgreSQLSourceFunction(ChunjunPostgreSQLSourceFactory.this);
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseChunjunDescriptor {
        @Override
        public IDataXPluginMeta.EndType getEndType() {
            return IDataXPluginMeta.EndType.Postgres;
        }

//        @Override
//        protected IDataXPluginMeta.EndType getSourceType() {
//            return IDataXPluginMeta.EndType.Postgres;
//        }
    }
}

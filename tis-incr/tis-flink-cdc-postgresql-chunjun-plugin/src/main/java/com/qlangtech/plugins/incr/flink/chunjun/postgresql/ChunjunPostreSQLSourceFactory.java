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

package com.qlangtech.plugins.incr.flink.chunjun.postgresql;

import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-27 08:06
 **/
public class ChunjunPostreSQLSourceFactory extends MQListenerFactory {

    private static final String DESC_NAME = "Flink-Chunjun-PostgreSQL";
    private transient IConsumerHandle<RowData, JobExecutionResult> consumerHandle;

    public IConsumerHandle getConsumerHandle() {
        return this.consumerHandle;
    }

    @Override
    public IMQListener create() {
        return new PostgreSQLSourceFunction(this);
    }

    @Override
    public void setConsumerHandle(IConsumerHandle consumerHandle) {
        this.consumerHandle = consumerHandle;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return DESC_NAME;
        }

        @Override
        public Optional<IDataXPluginMeta.EndType> getTargetType() {
            return Optional.of(IDataXPluginMeta.EndType.Postgres);
        }
    }
}

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

package com.qlangtech.tis.plugins.incr.flink.connector.paimon.sink;

import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-14 12:16
 **/
public class PaimonSinkFactory extends BasicTISSinkFactory<org.apache.flink.cdc.common.event.Event> {
    @Override
    public Map<TableAlias, TabSinkFunc<org.apache.flink.cdc.common.event.Event>>
    createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator) {
        return Map.of();
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return null;
    }

    @TISExtension
    public static class PaimonDescriptor extends BaseSinkFunctionDescriptor {
        public PaimonDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Sink-" + getTargetType().name();
        }

        @Override
        protected EndType getTargetType() {
            return EndType.Paimon;
        }
    }
}

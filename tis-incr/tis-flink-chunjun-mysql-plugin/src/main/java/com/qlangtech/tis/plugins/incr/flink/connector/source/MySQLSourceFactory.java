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

package com.qlangtech.tis.plugins.incr.flink.connector.source;

import com.qlangtech.tis.plugins.incr.flink.chunjun.source.ChunjunSourceFactory;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-14 22:10
 **/
public class MySQLSourceFactory extends ChunjunSourceFactory {
    @Override
    public IMQListener create() {
        return new MySQLSourceFunction(this);
    }

    @TISExtension
    public static class DefaultDesc extends BaseChunjunDescriptor {
        @Override
        public IEndTypeGetter.EndType getEndType() {
            return IEndTypeGetter.EndType.MySQL;
        }
    }
}

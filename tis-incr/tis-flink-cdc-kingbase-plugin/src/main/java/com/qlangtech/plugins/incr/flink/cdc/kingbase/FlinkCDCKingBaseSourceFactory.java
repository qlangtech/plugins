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

package com.qlangtech.plugins.incr.flink.cdc.kingbase;

import com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFactory;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 19:08
 **/
public class FlinkCDCKingBaseSourceFactory extends FlinkCDCPGLikeSourceFactory {

    @Override
    public IMQListener create() {
        return new FlinkCDCKingBaseSourceFunction(this);
    }

    @TISExtension()
    public static class KingBaseDescriptor extends BasePGLikeDescriptor {
        @Override
        public String getDisplayName() {
            return "Flink-CDC-KingBase";
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.TIS;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return EndType.KingBase;
        }

    }
}

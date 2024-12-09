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

package com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.startup;

import com.qlangtech.plugins.incr.flink.cdc.mongdb.MongoCDCStartupOptions;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 14:14
 **/
public class LatestOffset extends MongoCDCStartupOptions {


    @Override
    public StartupOptions getOptionsType() {
        return option();
    }

    public static StartupOptions option() {
        return StartupOptions.latest();
    }

    @TISExtension
    public static final class DftDescriptor extends Descriptor<MongoCDCStartupOptions> {
        public DftDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return option().startupMode.name();
        }
    }
}

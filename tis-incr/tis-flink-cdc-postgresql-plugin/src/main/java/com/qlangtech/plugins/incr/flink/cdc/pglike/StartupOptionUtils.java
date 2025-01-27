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

package com.qlangtech.plugins.incr.flink.cdc.pglike;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 22:27
 **/
public class StartupOptionUtils {

    public static final String KEY_STARTUP_LATEST = "latest";

    public static StartupOptions getStartupOptions(String startupOptions) {
        switch (startupOptions) {
            case KEY_STARTUP_LATEST:
                return StartupOptions.latest();
//            case "earliest":
//                return StartupOptions.earliest();
            case "initial":
                return StartupOptions.initial();
            default:
                throw new IllegalStateException("illegal startupOptions:" + startupOptions);
        }
    }
}

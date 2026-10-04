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
package com.qlangtech.tis.plugin.ontology.workshop.model.event;

import com.qlangtech.tis.extension.TISExtension;

/** 开启模块自动刷新（前端 {@code enable-auto-refresh}）。无载荷。 */
public class EnableAutoRefreshConfig extends EventConfig {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Enable_Auto_Refresh";
        }

        @Override
        public String shortComment() {
            return "开启自动刷新";
        }
    }
}

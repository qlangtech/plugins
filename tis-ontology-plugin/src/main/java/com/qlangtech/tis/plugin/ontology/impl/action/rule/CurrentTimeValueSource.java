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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.extension.TISExtension;

import java.util.Map;

/**
 * 取当前时间。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/12
 */
public class CurrentTimeValueSource extends ValueSourceConfig {

    @Override
    public Object resolveValue(Map<String, Object> parameters) {
        return System.currentTimeMillis();
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor {

        @Override
        public String getDisplayName() {
            return "当前时间";
        }
    }
}
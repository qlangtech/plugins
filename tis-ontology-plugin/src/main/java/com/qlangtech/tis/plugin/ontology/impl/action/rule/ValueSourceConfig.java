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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.util.Map;

/**
 * 取值来源抽象基类。
 *
 * <p>用于 {@link PropertyMappingConfig} 的 {@code source} 字段，
 * 取代原先的枚举 + 条件字段的耦合设计。
 *
 * <p>三个具体子类：
 * <ul>
 *   <li>{@link FromParameterValueSource} - 从 Action 参数池取值</li>
 *   <li>{@link CurrentTimeValueSource} - 取当前时间</li>
 *   <li>{@link LiteralValueSource} - 固定字面量</li>
 * </ul>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/12
 */
public abstract class ValueSourceConfig implements Describable<ValueSourceConfig> {

    /**
     * 解析取值。
     *
     * @param parameters Action 参数池的运行时取值
     * @return 解析出的值；如果无法解析（如参数不存在）返回 null
     */
    public abstract Object resolveValue(Map<String, Object> parameters);

    public static abstract class BaseDescriptor extends Descriptor<ValueSourceConfig> {
    }
}
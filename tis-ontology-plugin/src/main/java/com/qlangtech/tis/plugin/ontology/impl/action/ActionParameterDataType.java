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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Action 参数的数据类型抽象基类（聚合属性插件）。
 *
 * <p>替代原先用枚举 + 条件字段的耦合设计，改为多态子类各自持有专属字段：
 * <ul>
 *   <li>{@link PrimitiveDataType} - 基本类型（STRING/INTEGER/DATE 等）
 *   <li>{@link ObjectReferenceDataType} - 对象引用类型
 * </ul>
 *
 * <p>这样的设计消除了字段间的条件依赖，提供编译期类型安全。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParameterConfig
 * @see PrimitiveDataType
 * @see ObjectReferenceDataType
 */
public abstract class ActionParameterDataType implements Describable<ActionParameterDataType> {

    /**
     * 返回该数据类型的运行时标识符，供日志、调试使用。
     *
     * @return 类型标识字符串，格式由子类定义
     */
    public abstract String getTypeIdentifier();

    /**
     * 聚合属性插件的基础描述符，所有子类的 Descriptor 必须继承此类。
     */
    protected abstract static class BasicDescriptor extends Descriptor<ActionParameterDataType> {
        // 公共描述符逻辑（当前为空，预留扩展点）
    }
}
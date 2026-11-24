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
import com.qlangtech.tis.plugin.ontology.OntologyType;

import java.util.Map;

/**
 * 取当前时间。
 *
 * <p>按映射行 target 的属性类型直出 JDBC 能正确序列化的类型：TIMESTAMP 属性返回
 * {@link java.sql.Timestamp}，其余（含类型未知，保守默认）返回 {@link java.sql.Date}
 * ——直接返回毫秒时间戳 Long 的话，{@link java.sql.PreparedStatement#setObject(int, Object)}
 * 会把数字原样发给 MySQL，DATE 列直接报 {@code Incorrect date value}；而
 * {@code java.sql.Date} 绑 DATETIME/TIMESTAMP 列时分秒会被补零。日期格式化按 JVM 默认时区。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/12
 */
public class CurrentTimeValueSource extends ValueSourceConfig {

    @Override
    public Object resolveValue(Map<String, Object> parameters, OntologyType colType) {
        long now = System.currentTimeMillis();
        return colType == OntologyType.TIMESTAMP ? new java.sql.Timestamp(now) : new java.sql.Date(now);
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor {

        @Override
        public String getDisplayName() {
            return "当前时间";
        }
    }
}
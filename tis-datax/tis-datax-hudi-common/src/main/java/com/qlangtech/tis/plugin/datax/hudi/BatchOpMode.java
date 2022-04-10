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

package com.qlangtech.tis.plugin.datax.hudi;

import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-05 07:00
 **/
public enum BatchOpMode {
    // WriteOperationType.UPSERT
    // WriteOperationType.INSERT
    // WriteOperationType.BULK_INSERT
    UPSERT("UPSERT", "upsert") //
    , INSERT("INSERT", "insert") //
    , BULK_INSERT("BULK_INSERT", "bulk_insert") //
    , INSERT_OVERWRITE("INSERT_OVERWRITE", "insert_overwrite");

    private final String value;
    public final String hudiType;

    private BatchOpMode(String value, String hudiType) {
        this.value = value;
        this.hudiType = hudiType;
    }

    public String getValue() {
        return value;
    }

    public static BatchOpMode parse(String value) {

        if (StringUtils.isEmpty(value)) {
            throw new IllegalArgumentException("argument value can not be empty");
        }
        for (BatchOpMode op : BatchOpMode.values()) {
            if (op.value.equals(value)) {
                return op;
            }
        }
        throw new IllegalStateException("invalid value:" + value);
    }
}

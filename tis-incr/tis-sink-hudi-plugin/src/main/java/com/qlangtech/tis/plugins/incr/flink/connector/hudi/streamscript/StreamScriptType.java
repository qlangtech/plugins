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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi.streamscript;

import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-24 10:36
 **/
public enum StreamScriptType {
    SQL("sql"), STREAM_API("streamApi");
    public final String val;

    private StreamScriptType(String val) {
        this.val = val;
    }

    public static StreamScriptType parse(String val) {
        if (StringUtils.isEmpty(val)) {
            throw new IllegalArgumentException("param val can not be empty");
        }
        for (StreamScriptType type : StreamScriptType.values()) {
            if (type.val.equals(val)) {
                return type;
            }
        }
        throw new IllegalStateException("illegal stream script val:" + val);
    }
}

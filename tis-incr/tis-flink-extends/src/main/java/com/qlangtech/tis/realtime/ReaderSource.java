/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.realtime;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-15 18:03
 **/
public class ReaderSource {
    public final SourceFunction<DTO> sourceFunc;
    public final String tokenName;

    public ReaderSource(String tokenName, SourceFunction<DTO> sourceFunc) {
        if (StringUtils.isEmpty(tokenName)) {
            throw new IllegalArgumentException("param tokenName can not be empty");
        }
        this.sourceFunc = sourceFunc;
        this.tokenName = tokenName;
    }
}

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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.qlangtech.tis.realtime.yarn.rpc.SynResTarget;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.easymock.IArgumentMatcher;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-22 22:39
 **/
public class SynResTargetMatcher implements IArgumentMatcher {
    private final SynResTarget expect;

    private SynResTarget actual;

    public SynResTargetMatcher(SynResTarget expect) {
        this.expect = Objects.requireNonNull(expect, "param expect can not be null");
    }

    @Override
    public boolean matches(Object argument) {
        if (argument instanceof SynResTarget) {
            actual = (SynResTarget) argument;
            return actual.isPipeline() == expect.isPipeline()
                    && StringUtils.equals(actual.getName(), expect.getName());
        }
        return false;
    }

    @Override
    public void appendTo(StringBuffer buffer) {
        buffer.append("expect:").append(ToStringBuilder.reflectionToString(expect))
                .append("\nactual:").append(ToStringBuilder.reflectionToString(actual));
    }
}

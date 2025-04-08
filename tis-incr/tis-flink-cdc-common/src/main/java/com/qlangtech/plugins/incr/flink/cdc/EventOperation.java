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

package com.qlangtech.plugins.incr.flink.cdc;

import io.debezium.data.Envelope.Operation;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-06 15:49
 **/
public class EventOperation {
    private final Operation op;
    private Object payload1;

    public <T> T getPayload1() {
        return (T) payload1;
    }

    /**
     * 存入和操作相关的 附属信息，在流程中可以利用该属性进行处理
     *
     * @param payload1
     */
    public EventOperation setPayload1(Object payload1) {
        this.payload1 = payload1;
        return this;
    }

    public EventOperation(Operation op) {
        this.op = op;
    }

    public boolean isInsert() {
        return op == Operation.CREATE || op == Operation.READ;
    }

    public boolean isUpdate() {
        return op == Operation.UPDATE;
    }

    public boolean isDelete() {
        return op == Operation.DELETE;
    }

    @Override
    public String toString() {
        return String.valueOf(op);
    }
}

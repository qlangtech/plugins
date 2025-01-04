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

package com.qlangtech.tis.plugin.kafka.consumer;

import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-04 16:25
 **/
public class KafkaStructuredRecord extends StructuredRecord {
    private EventType eventType;
    private Map<String, Object> oldVals;


    public void clean() {
        this.vals = null;
        this.eventType = null;
        this.oldVals = null;
        this.tabName = null;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Map<String, Object> getOldVals() {
        return oldVals;
    }

    public void setOldVals(Map<String, Object> oldVals) {
        this.oldVals = oldVals;
    }
}

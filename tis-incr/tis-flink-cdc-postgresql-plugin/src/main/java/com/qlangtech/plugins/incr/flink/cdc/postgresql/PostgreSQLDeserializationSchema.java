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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.qlangtech.plugins.incr.flink.cdc.DefaultTableNameConvert;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.kafka.connect.data.Struct;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-02-21 19:02
 **/
public class PostgreSQLDeserializationSchema extends TISDeserializationSchema {

    public PostgreSQLDeserializationSchema(List<ISelectedTab> tabs) {
        super(new PGDTOColValProcess(tabs), new DefaultTableNameConvert());
    }

    @Override
    protected Struct getBeforeVal(Struct value) {
        Struct beforeVal = super.getBeforeVal(value);
        if (beforeVal == null) {
            throw new IllegalStateException("lack before vals  ,for resolve this issue:https://developer.aliyun.com/ask/575334 \n shall execute alter: ALTER TABLE your_table_name REPLICA IDENTITY FULL;");
        }
        return beforeVal;
    }
}

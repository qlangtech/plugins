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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.realtime.transfer.DTO;

import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import org.apache.flink.types.RowKind;

/**
 * MongoDB 发生更新时候，before数据获取策略，目前有两种方式
 * 1. Full Changelog
 * 2. updateLookup 通过 CDC内部合并更新内容和更新之前的整条记录值
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 13:55
 * @see org.apache.flink.cdc.connectors.mongodb.MongoDBSource.Builder#updateLookup
 **/
public abstract class UpdateRecordComplete implements Describable<UpdateRecordComplete> {

    /**
     * 取得增量更新时，能得到的event记录类型
     * @return
     */
    public abstract RowKind[] getUpdateRowkindsForTest();

    public abstract void setProperty(MongoDBSourceBuilder<DTO> builder);

}

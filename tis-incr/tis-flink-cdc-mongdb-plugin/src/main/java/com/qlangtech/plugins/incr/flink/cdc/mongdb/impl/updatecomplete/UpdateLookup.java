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

package com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.updatecomplete;

import com.qlangtech.plugins.incr.flink.cdc.mongdb.UpdateRecordComplete;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource.Builder;
import org.apache.flink.types.RowKind;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 14:10
 **/
public class UpdateLookup extends UpdateRecordComplete {

    @Override
    public RowKind[] getUpdateRowkindsForTest() {
        // UpdateLookup策略之后有after类别的值
        return new RowKind[]{ /**RowKind.UPDATE_BEFORE,*/RowKind.UPDATE_AFTER};
    }

    @Override
    public void setProperty(Builder<DTO> builder) {
        builder.scanFullChangelog(false);
        builder.updateLookup(true);
    }

    @TISExtension
    public static final class DftDescriptor extends Descriptor<UpdateRecordComplete> {
        public DftDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "UPDATE_LOOKUP";
        }
    }
}

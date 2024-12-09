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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-15 17:10
 **/
public class RowValsUpdate extends RowVals<RowValsUpdate.UpdatedColVal> {
    private final TestRow testRow;

    public RowValsUpdate(TestRow testRow) {
        this.testRow = Objects.requireNonNull(testRow, "testRow can not be null");
    }

    public void put(String key, TestRow.ColValSetter val) {
        super.put(key, new UpdatedColVal(val, testRow.getColMetaMapper(key)));
    }

    public static class UpdatedColVal implements Callable<Object> {
        public final TestRow.ColValSetter updateStrategy;
        public RowValsExample.RowVal updatedVal;
        private ColMeta colMeta;

        @Override
        public Object call() throws Exception {
            return Objects.requireNonNull(updatedVal
                    , "updatedVal can not be null").call();
        }

        public UpdatedColVal(TestRow.ColValSetter updateStrategy, ColMeta colMeta) {
            this.updateStrategy = updateStrategy;
            this.colMeta = colMeta;
        }

        public void setPrepColVal(IStatementSetter statement, RowValsExample vals) throws Exception {
            this.updatedVal = updateStrategy.setPrepColVal(colMeta, statement, vals);
            Objects.requireNonNull(this.updatedVal, "updatedVal");
        }
    }
}

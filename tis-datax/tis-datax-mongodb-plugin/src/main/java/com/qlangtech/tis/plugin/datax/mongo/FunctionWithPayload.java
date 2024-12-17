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

package com.qlangtech.tis.plugin.datax.mongo;

import org.bson.BsonValue;

import java.io.Serializable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-09 14:04
 **/
public interface FunctionWithPayload extends Serializable {

    public abstract class MongoColValCreator {
        final FunctionWithPayload valCreator;
        private FunctionWithPayloadColumnDecorator _columnValueCreator;

        public MongoColValCreator(FunctionWithPayload valCreator) {
            this.valCreator = valCreator;
        }

        public final FunctionWithPayloadColumnDecorator getColumnValueCreator() {
            if (this._columnValueCreator == null) {
                this._columnValueCreator = this.createColumnValueCreator();
            }
            return this._columnValueCreator;
        }

        protected abstract FunctionWithPayloadColumnDecorator createColumnValueCreator();

    }

    /**
     * @param o
     * @param payloads 额外传输参数，例如timeZone等
     * @return
     */
    public Object apply(BsonValue o, Object... payloads);


}

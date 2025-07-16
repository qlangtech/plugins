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

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.time.ZoneId;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-10 16:33
 **/
public abstract class MongoColValGetter<RESULT_TYPE> implements Function<BsonDocument, RESULT_TYPE> {

    protected final MongoCMeta cmeta;
    protected final ZoneId zone;
    // protected final boolean getNestVal;

    public MongoColValGetter(MongoCMeta cmeta, ZoneId zone) {
        this.cmeta = Objects.requireNonNull(cmeta, "cmeta can not be null");
        this.zone = Objects.requireNonNull(zone, "zone can not be null");
    }

    /**
     * @param cMeta
     * @param dataXColumType
     * @return
     */
    public static MongoColValGetter create(MongoCMeta cMeta, boolean dataXColumType, ZoneId zone) {
        return dataXColumType ? new DataXColumnValGetter(cMeta, zone) : new FlinkPropValGetter(cMeta, zone);
    }

    /**
     * 返回最终primative 的 值
     */
    private static class FlinkPropValGetter extends MongoColValGetter<Object> {
        public FlinkPropValGetter(MongoCMeta cmeta, ZoneId zone) {
            super(cmeta, zone);
        }

        @Override
        public Object apply(BsonDocument document) {
            BsonValue val = cmeta.getBsonVal(document);
            if (val == null || (val.getBsonType() == BsonType.NULL)) {
                return null;
            }
            return MongoDataXColUtils.createCol(cmeta, val, false, zone);
        }
    }


}

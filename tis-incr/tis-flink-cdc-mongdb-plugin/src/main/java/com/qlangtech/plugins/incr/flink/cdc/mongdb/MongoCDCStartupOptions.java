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
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceBuilder;

/**
 * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#startup-reading-position
 * MongoDBSourceBuilder<DTO> builder
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 14:08
 **/
public abstract class MongoCDCStartupOptions implements Describable<MongoCDCStartupOptions> {

    protected abstract StartupOptions getOptionsType();

    public final void setProperty(MongoDBSourceBuilder<DTO> builder) {
        builder.startupOptions(this.getOptionsType());
        this.appendProperty(builder);
    }

    protected void appendProperty(MongoDBSourceBuilder<DTO> builder) {
    }

}

/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.chunjun.greenplum.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.greenplum.source.GreenplumSourceFactory;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-14 21:26
 **/
public class ExtendGreenplumSourceFactory extends GreenplumSourceFactory {
    private final DataSourceFactory dataSourceFactory;

    public ExtendGreenplumSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env, DataSourceFactory dataSourceFactory) {
        super(syncConf, env);
        this.fieldList = syncConf.getReader().getFieldList();
        this.dataSourceFactory = dataSourceFactory;
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new TISGreenplumInputFormat(dataSourceFactory));
    }

}

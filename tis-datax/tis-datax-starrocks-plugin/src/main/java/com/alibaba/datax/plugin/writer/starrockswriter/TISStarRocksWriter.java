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

package com.alibaba.datax.plugin.writer.starrockswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.common.RdbmsWriterErrorCode;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.starrocks.connector.datax.plugin.writer.starrockswriter.StarRocksWriter;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-09-19 09:54
 **/
public class TISStarRocksWriter extends StarRocksWriter {
    public static class Job extends StarRocksWriter.Job {
        @Override
        public void init() {
            Configuration cfg = super.getPluginJobConf();
            // 判断表是否存在，如果不存在则创建表
            //final String dataXName = cfg.getNecessaryValue(StoreResourceType.DATAX_NAME, RdbmsWriterErrorCode.REQUIRED_DATAX_PARAM_ERROR);
            String tableName = cfg.getNecessaryValue(com.alibaba.datax.plugin.rdbms.writer.Key.TABLE
                    , RdbmsWriterErrorCode.REQUIRED_TABLE_NAME_PARAM_ERROR);
            List<String> jdbcUrls = Lists.newArrayList(
                    cfg.getNecessaryValue(Key.JDBC_URL, RdbmsWriterErrorCode.REQUIRED_TABLE_NAME_PARAM_ERROR));
            try {

                DataxWriter.process(this.containerContext, tableName, jdbcUrls);
            } catch (Exception e) {
                throw DataXException.asDataXException(RdbmsWriterErrorCode.INITIALIZE_TABLE_ERROR, tableName, e);
            }

            super.init();
        }

    }

    public static class Task extends StarRocksWriter.Task {

    }
}

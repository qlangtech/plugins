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

package com.qlangtech.tis.plugin.datax.writer;

import com.alibaba.datax.common.element.QueryCriteria;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.ThreadLocalRows;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * 用于在页面上数据预览功能
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-21 07:44
 **/
public class DataGridWriter extends Writer {
    public static class Job extends Writer.Job {
        // private Configuration originalConfig;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // Record.threadLocalRows.remove();
            // 校验不能为空
            // 由 EmbeddedDataXJobSubmit 类内的 previewRowsData 方法负责注入
            Objects.requireNonNull(this.containerContext.getAttr(ThreadLocalRows.class));
            List<Configuration> cfgs = Lists.newArrayList();
            for (int i = 0; i < mandatoryNumber; i++) {
                cfgs.add(super.getPluginJobConf().clone());
            }
            return cfgs;
            //  return Collections.singletonList(super.getPluginJobConf());
        }

        @Override
        public void init() {
            //   this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Writer.Task {

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            ThreadLocalRows localRows = this.containerContext.getAttr(ThreadLocalRows.class);

            final QueryCriteria queryCriteria = localRows.getQuery();
            Record record = null;
            int readCount = 0;
            while ((record = lineReceiver.getFromReader()) != null) {
                localRows.addRecord(record);
                // gridRows.add(record);
                //recordToString(record);
                if (++readCount > queryCriteria.getPageSize()) {
                    // throw new IllegalStateException("readCount:" + readCount + " can not more than page size:" + queryCriteria.getPageSize());
                    return;
                }
            }
        }


        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }
    }
}

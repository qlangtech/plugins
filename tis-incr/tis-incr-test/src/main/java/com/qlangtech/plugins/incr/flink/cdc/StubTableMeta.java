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

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.qlangtech.tis.datax.IStreamTableCreator;

import java.io.Serializable;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-01 09:50
 **/
public class StubTableMeta implements IStreamTableCreator.IStreamTableMeta, Serializable {
    final List<HdfsColMeta> cols;

    public StubTableMeta(List<HdfsColMeta> cols) {
        this.cols = cols;
    }

    @Override
    public List<HdfsColMeta> getColsMeta() {
        return this.cols;
    }
}

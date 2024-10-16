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

package com.qlangtech.tis.plugin.datax.seq;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;

/**
 * https://doris.apache.org/docs/dev/data-operate/update-delete/sequence-column-manual?_highlight=seq
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-31 15:05
 **/
public abstract class SeqKey implements Describable<SeqKey> {

    public StringBuffer createDDLScript(AbstractCreateTableSqlBuilder<? extends ColWrapper> tableMapper) {
        return new StringBuffer();
    }

    /**
     * 是否已经开启时间序列功能
     *
     * @return
     */
    public abstract boolean isOn();

    public String getSeqColName() {
        throw new UnsupportedOperationException();
    }

    public abstract void appendBatchCfgs(JSONObject props);
}

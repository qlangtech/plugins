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

package com.qlangtech.plugins.incr.flink.cdc.pglike;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtensible;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.io.Serializable;
import java.util.List;

/**
 * PostgreSQL binlog的复制规则，如果需要支持物理删除，必须要使用full，不然更新流程中不会附带需要删除的物理id
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-11 16:31
 **/
@TISExtensible
public abstract class PGLikeReplicaIdentity implements Describable<PGLikeReplicaIdentity>, Serializable {
    protected static final String FULL = "FULL";
    protected static final String DEFAULT = "DEFAULT";

    public abstract boolean isShallContainBeforeVals();


    public abstract boolean validateSelectedTabs(
            IControlMsgHandler msgHandler
            , Context context
            , IDataSourceFactoryGetter dataSourceGetter, List<ISelectedTab> selectedTabs);
}

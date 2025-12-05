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

package com.qlangtech.tis.plugin.datax.kingbase;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.DataXPostgresqlWriter;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.ds.kingbase.BasicKingBaseDataSourceFactory;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 15:05
 **/
public class DataXKingBaseWriter extends DataXPostgresqlWriter {

    @Override
    public BasicKingBaseDataSourceFactory getDataSourceFactory() {
        return (BasicKingBaseDataSourceFactory) super.getDataSourceFactory();
    }


    @Override
    public AutoCreateTable getAutoCreateTableCanNotBeNull() {
        return Objects.requireNonNull(this.getDataSourceFactory().dbMode.autoCreateTable, "autoCreateTable can not be null");
    }

    @TISExtension()
    public static class KingBaseDescriptor extends DefaultDescriptor {
        public KingBaseDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return BasicKingBaseDataSourceFactory.KingBase_NAME;
        }

        @Override
        public EndType getEndType() {
            return EndType.KingBase;
        }
    }
}

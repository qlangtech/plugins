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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-02-07 12:03
 **/
public class KingBaseMySQLModeAutoCreateTable extends MySQLAutoCreateTable {

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        return new KingBaseMySQLModeCreateTableSqlBuilder(sourceColMetaGetter, tableMapper, rdbmsWriter, transformers);
    }

    protected class KingBaseMySQLModeCreateTableSqlBuilder extends MySQLCreateTableSqlBuilder {
        public KingBaseMySQLModeCreateTableSqlBuilder(SourceColMetaGetter sourceColMetaGetter
                , TableMap tableMapper, DataxWriter rdbmsWriter, Optional<RecordTransformerRules> transformers) {
            super(sourceColMetaGetter, tableMapper, rdbmsWriter, transformers);
        }

        @Override
        protected void appendTabMeta(List<String> pks) {
            //  script.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci").append("\n");
        }
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {
        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.KingBase;
        }

        @Override
        public Optional<EndType> getCompatibleModeEndType() {
            return Optional.of(EndType.MySQL);
        }
    }
}

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

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtensible;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 15:23
 * // @see com.qlangtech.tis.plugin.datax.kingbase.mode.MySQLMode
 * // @see com.qlangtech.tis.plugin.datax.kingbase.mode.OracleMode
 * // @see com.qlangtech.tis.plugin.datax.kingbase.mode.PGMode
 **/
@TISExtensible
public abstract class KingBaseCompatibleMode implements Describable<KingBaseCompatibleMode>, Serializable {

    public abstract EndType getEndType();

    public boolean isEndTypeMatch(final String dbMode) {
        // final String dbMode = result.getString("database_mode");
        EndType expectEndType = null;
        switch (dbMode) {
            case "oracle":
                expectEndType = EndType.Oracle;
                break;
            case "mysql":
                expectEndType = EndType.MySQL;
                break;
            case "pg":
                expectEndType = EndType.Postgres;
                break;
            default:
                throw new IllegalStateException("unsupported dbMode:" + dbMode);
        }
        return this.getEndType() == expectEndType;
    }


    public abstract Optional<String> getEscapeChar();

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public AutoCreateTable autoCreateTable;

    /**
     * 需要是继承 <b>com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect</b> 的类
     *
     * @return
     */
    public abstract Class<?> getJdbcDialectClass();

    public final CreateTableSqlBuilder<ColWrapper> createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        return autoCreateTable.createSQLDDLBuilder(rdbmsWriter, sourceColMetaGetter, tableMapper, transformers);
    }
}

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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.datax.common.RdbmsWriterContext;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 10:46
 **/
public class SqlServerWriterContext extends RdbmsWriterContext<DataXSqlserverWriter, SqlServerDatasourceFactory> {
    public static final String EscapeChar = "\\\"";
    public SqlServerWriterContext(DataXSqlserverWriter writer, IDataxProcessor.TableMap tabMapper, Optional<RecordTransformerRules> transformerRules) {
        super(writer, tabMapper,transformerRules);
    }

    @Override
    protected String colEscapeChar() {
        return EscapeChar;
    }
}

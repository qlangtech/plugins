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

package com.qlangtech.tis.plugins.incr.flink.connector.kingbase.dialect;


import com.dtstack.chunjun.connector.jdbc.converter.JdbcRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleRowConverter extends JdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public static final int CLOB_LENGTH = 4000;

    protected ArrayList<IDeserializationConverter> toAsyncInternalConverters;

    public OracleRowConverter(
            int fieldCount, List<IDeserializationConverter> toInternalConverters
            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters
    ) {
        super(fieldCount, toInternalConverters, toExternalConverters);

        toAsyncInternalConverters = new ArrayList<>(fieldCount);
        for (Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType> p : toExternalConverters) {
            toAsyncInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createAsyncInternalConverter(p.getValue())));
        }

    }

    protected IDeserializationConverter createAsyncInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return val -> val;
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            default:
                return createInternalConverter(type);
        }
    }


}


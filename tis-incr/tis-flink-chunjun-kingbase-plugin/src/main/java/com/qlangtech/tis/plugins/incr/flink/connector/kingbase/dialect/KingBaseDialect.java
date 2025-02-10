///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.qlangtech.tis.plugins.incr.flink.connector.kingbase.dialect;
//
//import com.dtstack.chunjun.conf.ChunJunCommonConf;
//import com.dtstack.chunjun.conf.SyncConf;
//import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
//import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
//import com.dtstack.chunjun.connector.jdbc.dialect.SupportUpdateMode;
//import com.dtstack.chunjun.connector.jdbc.sink.IFieldNamesAttachedStatement;
//import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
//import com.dtstack.chunjun.converter.AbstractRowConverter;
//import com.dtstack.chunjun.converter.IDeserializationConverter;
//import com.dtstack.chunjun.converter.ISerializationConverter;
//import com.dtstack.chunjun.converter.RawTypeConverter;
//import com.dtstack.chunjun.sink.WriteMode;
//import com.qlangtech.tis.plugin.ds.kingbase.KingBaseDataSourceFactory;
//import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
//import io.vertx.core.json.JsonArray;
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
//import org.apache.flink.table.types.logical.LogicalType;
//import org.apache.flink.table.types.logical.RowType;
//
//import java.sql.ResultSet;
//import java.util.List;
//import java.util.Optional;
//import java.util.stream.Collectors;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-01-25 09:56
// * @see //OracleDialect
// **/
//@SupportUpdateMode(modes = {WriteMode.INSERT, WriteMode.UPDATE, WriteMode.UPSERT})
//public class KingBaseDialect implements JdbcDialect {
//
//    private final JdbcConf jdbcConf;
//
//    public KingBaseDialect(SyncConf syncConf) {
//        this.jdbcConf = JdbcSinkFactory.getJdbcConf(syncConf);
//    }
//
//    @Override
//    public String dialectName() {
//        return "KingBase";
//    }
//
//    @Override
//    public boolean canHandle(String url) {
//        return url.startsWith("jdbc:" + KingBaseDataSourceFactory.JDBC_SCHEMA_TYPE + ":");
//    }
//
//    @Override
//    public Optional<String> getReplaceStatement(String schema, String tableName, List<String> fieldNames) {
//        if (CollectionUtils.isEmpty(jdbcConf.getUniqueKey())) {
//            throw new IllegalArgumentException("jdbcConf.getUniqueKey() can not be empty");
//        }
//        return getUpsertStatement(schema, tableName, fieldNames, jdbcConf.getUniqueKey(), false);
//    }
//
//    @Override
//    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
//    getRowConverter(
//            int fieldCount, List<IDeserializationConverter> toInternalConverters
//            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters) {
//        return new KingBaseRowConverter(fieldCount, toInternalConverters, toExternalConverters);
//    }
//
//
//    @Override
//    public AbstractRowConverter<ResultSet, JsonArray, IFieldNamesAttachedStatement, LogicalType> getColumnConverter(
//            ChunJunCommonConf commonConf, int fieldCount, List<IDeserializationConverter> toInternalConverters
//            , List<Pair<ISerializationConverter<IFieldNamesAttachedStatement>, LogicalType>> toExternalConverters) {
//        return new KingBaseColumnConverter(commonConf, fieldCount, toInternalConverters, toExternalConverters);
//    }
//
////    @Override
////    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
////    getColumnConverter(RowType rowType, ChunJunCommonConf commonConf) {
////        return JdbcDialect.super.getColumnConverter(rowType, commonConf);
////    }
//
//    @Override
//    public Optional<String> getUpsertStatement(
//            String schema,
//            String tableName,
//            List<String> fieldNames,
//            List<String> uniqueKeyFields,
//            boolean allReplace) {
//        tableName = buildTableInfoWithSchema(schema, tableName);
//        StringBuilder mergeIntoSql = new StringBuilder(64);
//        mergeIntoSql
//                .append("MERGE INTO ")
//                .append(tableName)
//                .append(" T1 USING (")
//                .append(buildDualQueryStatement(fieldNames))
//                .append(") T2 ON (")
//                .append(buildEqualConditions(uniqueKeyFields))
//                .append(") ");
//
//        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);
//
//        if (StringUtils.isNotEmpty(updateSql)) {
//            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
//            mergeIntoSql.append(updateSql);
//        }
//
//        mergeIntoSql
//                .append(" WHEN NOT MATCHED THEN ")
//                .append("INSERT (")
//                .append(
//                        (fieldNames.stream())
//                                .map(this::quoteIdentifier)
//                                .collect(Collectors.joining(", ")))
//                .append(") VALUES (")
//                .append(
//                        (fieldNames.stream())
//                                .map(col -> "T2." + quoteIdentifier(col))
//                                .collect(Collectors.joining(", ")))
//                .append(")");
//
//        return Optional.of(mergeIntoSql.toString());
//    }
//
//    /**
//     * build T1."A"=T2."A" or T1."A"=nvl(T2."A",T1."A")
//     */
//    private String buildUpdateConnection(
//            List<String> fieldNames, List<String> uniqueKeyFields, boolean allReplace) {
//        //List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
//        return (fieldNames.stream())
//                .filter(col -> !uniqueKeyFields.contains(col))
//                .map(col -> buildConnectString(allReplace, col))
//                .collect(Collectors.joining(","));
//    }
//
//    private String buildEqualConditions(List<String> uniqueKeyFields) {
//        return (uniqueKeyFields.stream())
//                .map(col -> "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col))
//                .collect(Collectors.joining(" and "));
//    }
//
//    /**
//     * Depending on parameter [allReplace] build different sql part. e.g T1."A"=T2."A" or
//     * T1."A"=nvl(T2."A",T1."A")
//     */
//    private String buildConnectString(boolean allReplace, String col) {
//        return allReplace
//                ? "T1." + quoteIdentifier(col) + " = T2." + quoteIdentifier(col)
//                : "T1."
//                + quoteIdentifier(col)
//                + " =NVL(T2."
//                + quoteIdentifier(col)
//                + ",T1."
//                + quoteIdentifier(col)
//                + ")";
//    }
//
//    public String buildDualQueryStatement(List<String> column) {
//        StringBuilder sb = new StringBuilder("SELECT ");
//        String collect =
//                column.stream()
//                        .map(col -> ":" + col + " " + quoteIdentifier(col))
//                        .collect(Collectors.joining(", "));
//        sb.append(collect).append(" FROM DUAL");
//        return sb.toString();
//    }
//
//    @Override
//    public RawTypeConverter getRawTypeConverter() {
//        return (colMeta) -> AbstractRowDataMapper.mapFlinkCol(colMeta, -1).type;
//    }
//}

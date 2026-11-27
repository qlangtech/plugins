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
package com.qlangtech.tis.plugin.ds.kingbase;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.CreateColumnMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 离线回归：金仓 JDBC 驱动 getColumns 对域类型列返回错误别名（现场 json 域/jsonb 基被报成
 * year/DATE，year 域/int4 基同样被报成 DATE，导致 JSON 文本当整数解析、Integer 转 LocalDate 失败）。
 * 本测试用代理 Connection/ResultSet 模拟 sys_catalog 递归查询，钉住 {@link KingBaseColumnTypes}
 * 的白名单纠正行为：只校正真实 json/jsonb 根类型与「类型链含 year 且根为整数」两类，其余一律不动。
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2026-11-27
 **/
public class TestKingBaseColumnTypes {

    private static final String SCHEMA = "pubtrans_ids";
    private static final String TABLE = "tis_cdc_type_probe";

    // ----------------------- 离线 fixture -----------------------

    /** 模拟 sys_catalog 递归 CTE 的行集：每行 [attname, typname, typbasetype]。 */
    private static ResultSet catalogRows(List<Object[]> rows) {
        int[] cursor = {-1};
        return (ResultSet) Proxy.newProxyInstance(TestKingBaseColumnTypes.class.getClassLoader()
                , new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            return ++cursor[0] < rows.size();
                        case "getString": {
                            Object v = rows.get(cursor[0])[(Integer) args[0] - 1];
                            return v == null ? null : v.toString();
                        }
                        case "getLong":
                            return ((Number) rows.get(cursor[0])[(Integer) args[0] - 1]).longValue();
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    /** 记录 SQL 文本与绑定参数的 Connection 代理。 */
    static class CatalogConnection {
        private final List<Object[]> rows = new ArrayList<>();
        private final Set<String> queries = new HashSet<>();
        private String sql;
        private String boundSchema;
        private String boundTable;

        /** 每个 chain 形如 {col, "year", "int4"}：非根行 typbasetype 非零，根行 typbasetype=0。 */
        CatalogConnection(Object[]... chains) {
            for (Object[] chain : chains) {
                String col = (String) chain[0];
                for (int i = 1; i < chain.length; i++) {
                    boolean root = (i + 1 == chain.length);
                    this.rows.add(new Object[]{col, chain[i], root ? 0L : 9000001L + i});
                }
            }
        }

        int queryCount() {
            return this.queries.size();
        }

        Connection proxy() {
            return (Connection) Proxy.newProxyInstance(TestKingBaseColumnTypes.class.getClassLoader()
                    , new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        if ("getSchema".equals(method.getName())) {
                            return SCHEMA;
                        }
                        if ("prepareStatement".equals(method.getName())) {
                            this.sql = (String) args[0];
                            return (PreparedStatement) Proxy.newProxyInstance(
                                    TestKingBaseColumnTypes.class.getClassLoader()
                                    , new Class<?>[]{PreparedStatement.class}, (p2, m2, a2) -> {
                                        if ("setString".equals(m2.getName())) {
                                            if ((Integer) a2[0] == 1) {
                                                this.boundSchema = (String) a2[1];
                                            } else {
                                                this.boundTable = (String) a2[1];
                                            }
                                            return null;
                                        }
                                        if ("executeQuery".equals(m2.getName())) {
                                            this.queries.add(this.sql);
                                            return catalogRows(this.rows);
                                        }
                                        return defaultValue(m2.getReturnType());
                                    });
                        }
                        return defaultValue(method.getReturnType());
                    });
        }
    }

    private static final Map<String, Integer> LABEL_INDEX = new HashMap<>();

    static {
        LABEL_INDEX.put("COLUMN_NAME", 4);
        LABEL_INDEX.put("DATA_TYPE", 5);
        LABEL_INDEX.put("TYPE_NAME", 6);
        LABEL_INDEX.put("COLUMN_SIZE", 7);
        LABEL_INDEX.put("DECIMAL_DIGITS", 9);
        LABEL_INDEX.put("NULLABLE", 11);
        LABEL_INDEX.put("REMARKS", 12);
    }

    /**
     * 模拟 DatabaseMetaData.getColumns 的单行（按 JDBC 规范索引/标签取值）：
     * 4=COLUMN_NAME 5=DATA_TYPE 6=TYPE_NAME 7=COLUMN_SIZE 9=DECIMAL_DIGITS 11=NULLABLE 12=REMARKS。
     */
    private static ResultSet getColumnsRow(String column, String typeName, int dataType, int columnSize, int decimalDigits) {
        Map<String, Object> values = new HashMap<>();
        values.put("COLUMN_NAME", column);
        values.put("DATA_TYPE", dataType);
        values.put("TYPE_NAME", typeName);
        values.put("COLUMN_SIZE", columnSize);
        values.put("DECIMAL_DIGITS", decimalDigits);
        values.put("NULLABLE", 1); // typeNullable
        values.put("REMARKS", null);
        return (ResultSet) Proxy.newProxyInstance(TestKingBaseColumnTypes.class.getClassLoader()
                , new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                    if (args != null && args.length == 1 && method.getName().startsWith("get")) {
                        Object key = (args[0] instanceof String) ? values.get(args[0]) : null;
                        if (key == null && args[0] instanceof Integer) {
                            for (Map.Entry<String, Integer> e : LABEL_INDEX.entrySet()) {
                                if (e.getValue().equals(args[0])) {
                                    key = values.get(e.getKey());
                                    break;
                                }
                            }
                        }
                        switch (method.getName()) {
                            case "getObject":
                                return key;
                            case "getString":
                                return key == null ? null : key.toString();
                            case "getInt":
                                return key == null ? 0 : ((Number) key).intValue();
                            case "getBoolean":
                                return key != null && ((Number) key).intValue() != 0;
                        }
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }

    /** 构建被测的 CreateColumnMeta（同包内 protected 方法可直接调用）。 */
    private static CreateColumnMeta createBuilder(CatalogConnection cat, ResultSet columnsRow) {
        BasicKingBaseDataSourceFactory dsFactory = new KingBaseDataSourceFactory();
        return dsFactory.createColumnMetaBuilder(EntityName.create(SCHEMA, TABLE), columnsRow
                , Collections.emptySet(), new JDBCConnection(cat.proxy(), "test"));
    }

    // ----------------------- KingBaseColumnTypes.read -----------------------

    @Test
    public void testYearDomainOverInt4CorrectedToInteger() throws SQLException {
        // 现场 c_year：year 域 / int4 基，getColumns 却报 DATE(91)
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_year", "year", "int4"}).proxy(), SCHEMA, TABLE);
        KingBaseColumnTypes.ColumnType cYear = types.get("c_year");
        Assert.assertNotNull("year 域列必须被纠正", cYear);
        Assert.assertEquals("int4", cYear.getTypeName());
        Assert.assertEquals(Types.INTEGER, cYear.getJdbcType());
        Assert.assertEquals("整数根在 TIS 字段模型保持整数", Types.INTEGER, cYear.getTisJdbcType());
    }

    @Test
    public void testYearDomainOverInt2AndInt8() throws SQLException {
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(
                        new Object[]{"c_smallyear", "year", "int2"},
                        new Object[]{"c_bigyear", "year", "int8"}).proxy(), SCHEMA, TABLE);
        Assert.assertEquals(Types.SMALLINT, types.get("c_smallyear").getJdbcType());
        Assert.assertEquals(Types.BIGINT, types.get("c_bigyear").getJdbcType());
    }

    @Test
    public void testJsonDomainOverJsonbRoot() throws SQLException {
        // 现场 c_json：json 域 / jsonb 基，getColumns 却报 year/DATE
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_json", "json", "jsonb"}).proxy(), SCHEMA, TABLE);
        KingBaseColumnTypes.ColumnType cJson = types.get("c_json");
        Assert.assertNotNull("json 域列必须被纠正", cJson);
        Assert.assertEquals("以解析到的真实根类型为准", "jsonb", cJson.getTypeName());
        Assert.assertEquals(Types.OTHER, cJson.getJdbcType());
        Assert.assertEquals("JSON 在 TIS 字段模型按文本走 VARCHAR", Types.VARCHAR, cJson.getTisJdbcType());
    }

    @Test
    public void testPlainColumnUntouched() throws SQLException {
        // 普通列（非域、根类型非 json/jsonb、链中无 year）不得出现在纠正结果里
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(
                        new Object[]{"c_date", "date"},
                        new Object[]{"c_int", "int4"}).proxy(), SCHEMA, TABLE);
        Assert.assertTrue("普通类型不做任何纠正：" + types, types.isEmpty());
    }

    @Test
    public void testYearDomainOverDateUntouched() throws SQLException {
        // 名为 year 的域挂载在 date 上与整数 YEAR 别名无关，必须保持 DATE 语义
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_year_date", "year", "date"}).proxy(), SCHEMA, TABLE);
        Assert.assertTrue("year 域 over date 不按整数处理：" + types, types.isEmpty());
    }

    @Test
    public void testJsonNamedDomainOverTextUntouched() throws SQLException {
        // 名为 json 的域挂载在 text 上：根类型非 json/jsonb，不得纠正
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_json_text", "json", "text"}).proxy(), SCHEMA, TABLE);
        Assert.assertTrue("json 域 over text 不做纠正：" + types, types.isEmpty());
    }

    @Test
    public void testOnlyRootRowIsAuthoritative() throws SQLException {
        // 只有解析到根（typbasetype=0）的行才具权威性：链 pg_payload -> json -> jsonb 按根 jsonb 纠正
        Map<String, KingBaseColumnTypes.ColumnType> types = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_payload", "pg_payload", "json", "jsonb"}).proxy(), SCHEMA, TABLE);
        Assert.assertEquals("jsonb", types.get("c_payload").getTypeName());
    }

    @Test
    public void testCatalogQueryBindsSchemaAndTable() throws SQLException {
        CatalogConnection cat = new CatalogConnection(new Object[]{"c_year", "year", "int4"});
        KingBaseColumnTypes.read(cat.proxy(), SCHEMA, TABLE);
        Assert.assertNotNull(cat.sql);
        Assert.assertTrue("目录查询必须用相等条件绑定 schema/table：" + cat.sql
                , cat.sql.contains("n.nspname=? AND c.relname=?"));
        Assert.assertEquals(SCHEMA, cat.boundSchema);
        Assert.assertEquals(TABLE, cat.boundTable);
        Assert.assertTrue("域解析须有递归深度上限", cat.sql.contains("depth<32"));
    }

    @Test
    public void testNullSchemaFallsBackToConnectionSchema() throws SQLException {
        CatalogConnection cat = new CatalogConnection(new Object[]{"c_year", "year", "int4"});
        KingBaseColumnTypes.read(cat.proxy(), null, TABLE);
        Assert.assertEquals("schema 为空时回退到 connection.getSchema()", SCHEMA, cat.boundSchema);
    }

    // ----------------------- ColumnType.correct 代理 -----------------------

    @Test
    public void testCorrectProxyInterceptsOnlyTypeNameAndDataType() throws SQLException {
        // ColumnType 构造器私有，经公开的 read() 取得实例
        KingBaseColumnTypes.ColumnType ct = KingBaseColumnTypes.read(
                new CatalogConnection(new Object[]{"c_year", "year", "int4"}).proxy(), SCHEMA, TABLE).get("c_year");
        ResultSet corrected = ct.correct(getColumnsRow("c_year", "year", Types.DATE, 4, 0));

        // 命中拦截：索引与标签两种取法都要生效
        Assert.assertEquals("int4", corrected.getString(6));
        Assert.assertEquals("int4", corrected.getString("TYPE_NAME"));
        Assert.assertEquals(Types.INTEGER, corrected.getInt(5));
        Assert.assertEquals(Types.INTEGER, corrected.getInt("DATA_TYPE"));
        Assert.assertEquals("int4", corrected.getObject(6));
        Assert.assertEquals(Types.INTEGER, corrected.getObject(5));

        // 未命中拦截：其余列委托原对象
        Assert.assertEquals("c_year", corrected.getString(4));
        Assert.assertEquals("c_year", corrected.getString("COLUMN_NAME"));
        Assert.assertEquals(4, corrected.getInt(7));
        Assert.assertEquals(0, corrected.getInt("DECIMAL_DIGITS"));
    }

    // ----------------------- BasicKingBaseDataSourceFactory 集成 -----------------------

    @Test
    public void testCreateColDataTypeCorrectsYearAndJson() throws SQLException {
        CatalogConnection cat = new CatalogConnection(
                new Object[]{"c_year", "year", "int4"},
                new Object[]{"c_json", "json", "jsonb"},
                new Object[]{"c_date", "date"});
        // 现场 getColumns 把 year/json 域列都报成 year/DATE(91)/length=4
        CreateColumnMeta builder = createBuilder(cat, getColumnsRow("c_year", "year", Types.DATE, 4, 0));

        DataType year = builder.create("c_year", 0).getType();
        Assert.assertEquals("c_year 必须按实际根类型 int4 建模", Types.INTEGER, year.getType());

        DataType json = builder.create("c_json", 1).getType();
        Assert.assertEquals("c_json 按 JSON 文本建模", Types.VARCHAR, json.getType());
        Assert.assertTrue("JSON 文本长度不得继承错误的 YEAR length=4，实得 " + json.getColumnSize()
                , json.getColumnSize() >= 65536);

        DataType date = builder.create("c_date", 2).getType();
        Assert.assertEquals("普通 DATE 列保持不变", Types.DATE, date.getType());
    }

    @Test
    public void testCatalogReadOncePerTableBuild() throws SQLException {
        CatalogConnection cat = new CatalogConnection(
                new Object[]{"c_year", "year", "int4"},
                new Object[]{"c_json", "json", "jsonb"});
        CreateColumnMeta builder = createBuilder(cat, getColumnsRow("c_year", "year", Types.DATE, 4, 0));
        builder.create("c_year", 0);
        Assert.assertEquals("首次构建触发一次目录查询", 1, cat.queryCount());
        builder.create("c_json", 1);
        Assert.assertEquals("同一 builder 内多个列共享一次目录读取", 1, cat.queryCount());
    }

    @Test
    public void testNumericScalePreserved() throws SQLException {
        CatalogConnection cat = new CatalogConnection(new Object[]{"c_numeric", "numeric"});
        CreateColumnMeta builder = createBuilder(cat, getColumnsRow("c_numeric", "numeric", Types.NUMERIC, 12, 2));
        DataType numeric = builder.create("c_numeric", 0).getType();
        Assert.assertEquals(Types.NUMERIC, numeric.getType());
        Assert.assertEquals("小数精度不得被纠正逻辑抹掉", 2, numeric.getDecimalDigits().intValue());
        Assert.assertEquals("无关列的长度保持原值", 12, numeric.getColumnSize());
    }

    @Test
    public void testCorrectedTypeRoundTripToColumnMetaData() throws SQLException {
        CatalogConnection cat = new CatalogConnection(new Object[]{"c_year", "year", "int4"});
        CreateColumnMeta builder = createBuilder(cat, getColumnsRow("c_year", "year", Types.DATE, 4, 0));
        ColumnMetaData colMeta = builder.create("c_year", 0);
        Assert.assertEquals("ColumnMetaData 携带纠正后的 INTEGER 类型", Types.INTEGER, colMeta.getType().getType());
        Assert.assertEquals("int4", colMeta.getType().typeName);
        Assert.assertFalse(colMeta.isPk());
    }
}

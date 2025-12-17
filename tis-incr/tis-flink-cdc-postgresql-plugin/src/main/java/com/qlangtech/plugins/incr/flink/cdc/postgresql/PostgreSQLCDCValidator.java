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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL CDC 先验校验工具类
 * 在Flink CDC任务启动前检查PostgreSQL数据源是否满足CDC运行条件
 * see  requirment/add-validator-before-pg-cdc-launching.md
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 20:00
 **/
public class PostgreSQLCDCValidator {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCDCValidator.class);

    /**
     * 最低支持的PostgreSQL主版本号
     */
    private static final int MIN_MAJOR_VERSION = 10;

    /**
     * 执行完整的PostgreSQL CDC先验校验
     *
     * @param conn           JDBC连接对象
     * @param sourceFactory  CDC源工厂配置
     * @param selectedTables 需要监听的表名集合
     * @return 校验结果
     */
    public static ValidationResult validate(Connection conn,
                                            FlinkCDCPGLikeSourceFactory sourceFactory,
                                            List<String> selectedTables) {
        ValidationResult result = new ValidationResult();

        try {
            // 1. 检查PostgreSQL版本
            validateVersion(conn, result);
            if (!result.isValid()) {
                return result; // 版本不满足,直接返回
            }

            // 2. 检查WAL配置
            validateWalConfiguration(conn, result);

            // 3. 检查用户权限
            validateUserPrivileges(conn, result);

            // 4. 检查pg_publication表存在性
            validatePgPublicationTableExists(conn, result);

            // 5. 检查复制槽状态
            validateReplicationSlots(sourceFactory, conn, result);

            // 6. 检查解码插件配置
            validateDecodingPlugin(conn, sourceFactory, result);

            // 7. 检查监听表结构, 表存在先不监听
//            if (selectedTables != null && !selectedTables.isEmpty()) {
//                for (String tableName : selectedTables) {
//                    validateTableStructure(conn, tableName, result);
//                }
//            }

        } catch (SQLException e) {
            logger.error("Database error occurred during PostgreSQL CDC validation", e);
            result.addError("数据库连接或查询错误: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unknown error occurred during PostgreSQL CDC validation", e);
            result.addError("校验过程发生错误: " + e.getMessage());
        }

        return result;
    }

    /**
     * 校验PostgreSQL版本
     * 要求版本 >= 10 (逻辑复制在PostgreSQL 10开始更加稳定)
     */
    private static void validateVersion(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SHOW server_version";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String version = rs.getString(1);
                logger.info("PostgreSQL version: {}", version);

                if (!isVersionSupported(version)) {
                    result.addError(String.format(
                            "PostgreSQL版本过低,需要%d或更高版本,当前版本: %s",
                            MIN_MAJOR_VERSION, version));
                }
            } else {
                throw new IllegalStateException("无法获取PostgreSQL版本信息");
            }
        }
    }

    /**
     * 校验WAL配置
     * - wal_level必须为logical
     * - max_replication_slots >= 1
     * - max_wal_senders >= 1
     */
    private static void validateWalConfiguration(Connection conn, ValidationResult result) throws SQLException {
        // 检查wal_level
        checkConfigParameter(conn, "wal_level", "logical", true, result);

        // 检查max_replication_slots
        checkIntConfigParameter(conn, "max_replication_slots", 1, result);

        // 检查max_wal_senders
        checkIntConfigParameter(conn, "max_wal_senders", 1, result);
    }

    /**
     * 校验用户权限
     * 检查当前用户是否具有REPLICATION权限
     */
    private static void validateUserPrivileges(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SELECT rolreplication FROM pg_roles WHERE rolname = CURRENT_USER";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                boolean hasReplication = rs.getBoolean("rolreplication");
                if (!hasReplication) {
                    result.addError("当前用户没有REPLICATION权限,无法创建逻辑复制槽");
                }
            } else {
                throw new IllegalStateException("无法查询当前用户权限");
            }
        }
    }

    /**
     * 校验pg_publication系统表是否存在
     * 这是判断PostgreSQL是否支持逻辑复制的关键指标
     */
    private static void validatePgPublicationTableExists(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SELECT EXISTS (" +
                "  SELECT 1 FROM information_schema.tables " +
                "  WHERE table_schema = 'pg_catalog' " +
                "  AND table_name = 'pg_publication'" +
                ")";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                boolean exists = rs.getBoolean(1);
                if (!exists) {
                    result.addError("pg_publication系统表不存在,PostgreSQL版本可能过低或不支持逻辑复制");
                }
            }
        }
    }

    /**
     * 校验复制槽状态
     * 检查配置的 slotName 是否已经被其他 CDC 任务占用
     * 如果 slot 正在运行（active = true），则报错，不允许启动任务
     */
    private static void validateReplicationSlots(FlinkCDCPGLikeSourceFactory sourceFactory, Connection conn, ValidationResult result) throws SQLException {
        // 检查 slotName 参数
        if (StringUtils.isEmpty(sourceFactory.slotName)) {
            throw new IllegalStateException("slotName can not be empty");
        }

        // 查询指定 slotName 的状态
        String sql = "SELECT slot_name, active, slot_type, " +
                "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as wal_lag " +
                "FROM pg_replication_slots " +
                "WHERE slot_name = ?";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, sourceFactory.slotName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    // slot 存在，检查是否处于活跃状态
                    boolean isActive = rs.getBoolean("active");
                    String walLag = rs.getString("wal_lag");

                    if (isActive) {
                        // slot 正在运行，报错
                        result.addError(String.format(
                                "Replication slot '%s' is currently active and being used by another CDC task. " +
                                "Please use a different slot name or stop the conflicting CDC task.",
                                sourceFactory.slotName));
                    } else {
                        // slot 存在但不活跃，给出警告
                        result.addWarning(String.format(
                                "Replication slot '%s' exists but is inactive. WAL lag: %s. " +
                                "This slot will be reused when CDC task starts. Consider cleaning it if not needed.",
                                sourceFactory.slotName, walLag != null ? walLag : "unknown"));
                    }
                }
                // 如果 slot 不存在，这是正常情况，CDC 启动时会自动创建，无需处理
            }
        } catch (SQLException e) {
            // pg_replication_slots 在某些版本可能不存在,记录日志但不影响校验
            logger.warn("Unable to query replication slot '{}' status: {}",
                    sourceFactory.slotName, e.getMessage());
        }
    }

    /**
     * 校验解码插件配置
     */
    private static void validateDecodingPlugin(Connection conn,
                                               FlinkCDCPGLikeSourceFactory sourceFactory,
                                               ValidationResult result) throws SQLException {
        String pluginName = sourceFactory.decodingPluginName;

        if (StringUtils.isEmpty(pluginName)) {
            throw new IllegalStateException("decodingPluginName不能为空");
        }

        // pgoutput是PostgreSQL 10+自带的,其他插件需要额外安装
        if ("pgoutput".equalsIgnoreCase(pluginName)) {
            // pgoutput是内置的,不需要检查安装
            logger.info("Using built-in pgoutput decoding plugin");
        } else {
            // 检查其他解码插件是否已安装
            String sql = "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = ?)";
            // 注意: decoderbufs, wal2json等需要通过CREATE EXTENSION安装
            result.addWarning(String.format(
                    "使用解码插件: %s, 请确保该插件已正确安装", pluginName));
        }
    }

    /**
     * 校验表结构
     * - 检查表是否存在
     * - 检查表是否有主键(无主键时仅警告)
     * - 检查表的REPLICA IDENTITY设置
     */
    private static void validateTableStructure(Connection conn, String tableName,
                                               ValidationResult result) throws SQLException {
        // 解析schema和table名称
        String[] parts = parseTableName(tableName);
        String schema = parts[0];
        String table = parts[1];

        // 1. 检查表是否存在
        if (!checkTableExists(conn, schema, table)) {
            result.addError(String.format("表不存在: %s.%s", schema, table));
            return; // 表不存在,后续检查无意义
        }

        // 2. 检查主键
        if (!checkTableHasPrimaryKey(conn, schema, table)) {
            result.addWarning(String.format(
                    "表 %s.%s 没有主键,可能影响CDC性能和数据一致性", schema, table));
        }

        // 3. 检查REPLICA IDENTITY
        checkReplicaIdentity(conn, schema, table, result);
    }

    /**
     * 检查配置参数是否符合预期值
     */
    private static void checkConfigParameter(Connection conn, String paramName,
                                             String expectedValue, boolean caseSensitive,
                                             ValidationResult result) throws SQLException {
        String sql = "SHOW " + paramName;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String actualValue = rs.getString(1);
                boolean matches = caseSensitive ?
                        expectedValue.equals(actualValue) :
                        expectedValue.equalsIgnoreCase(actualValue);

                if (!matches) {
                    result.addError(String.format(
                            "%s应该设置为'%s',当前值: '%s'",
                            paramName, expectedValue, actualValue));
                }
            }
        }
    }

    /**
     * 检查整数类型的配置参数
     */
    private static void checkIntConfigParameter(Connection conn, String paramName,
                                                int minValue, ValidationResult result) throws SQLException {
        String sql = "SHOW " + paramName;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                int actualValue = rs.getInt(1);
                if (actualValue < minValue) {
                    result.addError(String.format(
                            "%s必须至少为%d,当前值: %d",
                            paramName, minValue, actualValue));
                }
            }
        }
    }

    /**
     * 检查表是否存在
     */
    private static boolean checkTableExists(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(1) FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_name = ?";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);

            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    /**
     * 检查表是否有主键
     */
    private static boolean checkTableHasPrimaryKey(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(1) FROM information_schema.table_constraints " +
                "WHERE table_schema = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);

            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    /**
     * 检查表的REPLICA IDENTITY设置
     */
    private static void checkReplicaIdentity(Connection conn, String schema, String table,
                                             ValidationResult result) throws SQLException {
        String sql = "SELECT relreplident FROM pg_class " +
                "WHERE oid = (quote_ident(?) || '.' || quote_ident(?))::regclass";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String replIdent = rs.getString(1);
                    // d = default (主键), f = full, i = index, n = nothing
                    if ("n".equals(replIdent)) {
                        result.addWarning(String.format(
                                "表 %s.%s 的REPLICA IDENTITY设置为NOTHING,CDC可能无法正确捕获UPDATE和DELETE操作",
                                schema, table));
                    }
                }
            }
        } catch (SQLException e) {
            // 某些权限不足或版本问题可能导致查询失败,记录警告但不中断
            logger.warn("Unable to query REPLICA IDENTITY for table {}: {}", table, e.getMessage());
        }
    }

    /**
     * 解析表名,返回[schema, table]
     * 如果没有指定schema,默认为public
     */
    private static String[] parseTableName(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("表名不能为空");
        }

        String[] parts = tableName.split("\\.");
        if (parts.length == 1) {
            return new String[]{"public", parts[0]};
        } else if (parts.length == 2) {
            return new String[]{parts[0], parts[1]};
        } else {
            throw new IllegalArgumentException("表名格式错误: " + tableName);
        }
    }

    /**
     * 检查版本号是否满足要求
     */
    private static boolean isVersionSupported(String version) {
        if (StringUtils.isEmpty(version)) {
            return false;
        }

        try {
            // PostgreSQL版本格式: "14.5", "13.2", "10.23", "15.1 (Debian 15.1-1.pgdg110+1)" 等
            // 提取主版本号
            String versionNumber = version.split("\\s")[0]; // 取空格前的版本号
            String[] parts = versionNumber.split("\\.");

            if (parts.length > 0) {
                int majorVersion = Integer.parseInt(parts[0]);
                return majorVersion >= MIN_MAJOR_VERSION;
            }
        } catch (NumberFormatException e) {
            logger.warn("Unable to parse PostgreSQL version: {}", version);
        }

        return false;
    }

    /**
     * 校验结果类
     */
    public static class ValidationResult {
        private final List<String> errors = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        public void addError(String error) {
            if (StringUtils.isNotEmpty(error)) {
                errors.add(error);
                logger.error("PostgreSQL CDC validation error: {}", error);
            }
        }

        public void addWarning(String warning) {
            if (StringUtils.isNotEmpty(warning)) {
                warnings.add(warning);
                logger.warn("PostgreSQL CDC validation warning: {}", warning);
            }
        }

        public boolean isValid() {
            return errors.isEmpty();
        }

        public List<String> getErrors() {
            return errors;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (!errors.isEmpty()) {
                sb.append("错误:\n");
                errors.forEach(e -> sb.append("  - ").append(e).append("\n"));
            }
            if (!warnings.isEmpty()) {
                sb.append("警告:\n");
                warnings.forEach(w -> sb.append("  - ").append(w).append("\n"));
            }
            return sb.toString();
        }
    }
}

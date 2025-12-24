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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import io.debezium.connector.oracle.OracleConnectorConfig;
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
 * Oracle CDC pre-launch validator
 * Validates Oracle database configuration before Flink CDC job starts
 * to prevent runtime failures due to missing prerequisites
 *
 * @author: baisui(baisui@qlangtech.com)
 * @create: 2025-12-23
 **/
public class OracleCDCValidator {
    private static final Logger logger = LoggerFactory.getLogger(OracleCDCValidator.class);

    /**
     * Minimum supported Oracle major version
     */
    private static final int MIN_MAJOR_VERSION = 11;

    /**
     * Execute complete Oracle CDC pre-launch validation
     *
     * @param conn              JDBC connection
     * @param sourceFactory     CDC source factory configuration
     * @param selectedTables    List of tables to monitor
     * @return validation result
     */
    public static ValidationResult validate(Connection conn,
                                            FlinkCDCOracleSourceFactory sourceFactory,
                                            List<String> selectedTables) {
        ValidationResult result = new ValidationResult();

        try {
            // 1. Check Oracle version
            validateVersion(conn, result);
            if (!result.isValid()) {
                return result; // Version not met, return directly
            }

            // 2. Check ARCHIVELOG mode
            validateArchiveLogMode(conn, result);

            // 3. Check supplemental logging (both database and table level)
            validateSupplementalLogging(conn, sourceFactory, selectedTables, result);

            // 4. Validate log mining strategy specific requirements
            validateLogMiningStrategy(conn, sourceFactory, result);

            // 5. Check user privileges
            validateUserPrivileges(conn, result);

        } catch (SQLException e) {
            logger.error("Database error occurred during Oracle CDC validation", e);
            result.addError("Database connection or query error: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unknown error occurred during Oracle CDC validation", e);
            result.addError("Validation process error: " + e.getMessage());
        }

        return result;
    }

    /**
     * Validate Oracle version
     * Requires version >= 11g for stable LogMiner support
     */
    private static void validateVersion(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SELECT version FROM v$instance";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String version = rs.getString(1);
                logger.info("Oracle version: {}", version);

                if (!isVersionSupported(version)) {
                    result.addError(String.format(
                            "Oracle version is too low, requires version %dg or higher, current version: %s",
                            MIN_MAJOR_VERSION, version));
                }
            } else {
                throw new IllegalStateException("Unable to retrieve Oracle version information");
            }
        }
    }

    /**
     * Validate ARCHIVELOG mode
     * LogMiner requires database to be in ARCHIVELOG mode
     */
    private static void validateArchiveLogMode(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SELECT log_mode FROM v$database";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String logMode = rs.getString(1);
                logger.info("Oracle log_mode: {}", logMode);

                if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
                    result.addError(String.format(
                            "Database must be in ARCHIVELOG mode for CDC to work, current mode: %s. " +
                            "Please run: ALTER DATABASE ARCHIVELOG;", logMode));
                }
            } else {
                throw new IllegalStateException("Unable to query database log mode");
            }
        }
    }

    /**
     * Validate supplemental logging configuration
     * Both database-level and table-level supplemental logging need to be checked
     */
    private static void validateSupplementalLogging(Connection conn,
                                                    FlinkCDCOracleSourceFactory sourceFactory,
                                                    List<String> selectedTables,
                                                    ValidationResult result) throws SQLException {
        // Check database-level minimal supplemental logging
        validateDatabaseSupplementalLogging(conn, result);

        // Check table-level supplemental logging for selected tables
        if (selectedTables != null && !selectedTables.isEmpty()) {
            for (String tableName : selectedTables) {
                validateTableSupplementalLogging(conn, tableName, result);
            }
        }
    }

    /**
     * Validate database-level supplemental logging
     * At minimum, minimal supplemental logging must be enabled
     */
    private static void validateDatabaseSupplementalLogging(Connection conn, ValidationResult result) throws SQLException {
        String sql = "SELECT supplemental_log_data_min FROM v$database";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String minSupLog = rs.getString(1);
                logger.info("Database minimal supplemental logging: {}", minSupLog);

                if (!"YES".equalsIgnoreCase(minSupLog)) {
                    result.addError(
                            "Database minimal supplemental logging is not enabled. " +
                            "Please run: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;");
                }
            }
        }
    }

    /**
     * Validate table-level supplemental logging
     * Tables need appropriate supplemental logging for CDC to capture changes
     */
    private static void validateTableSupplementalLogging(Connection conn, String tableName,
                                                         ValidationResult result) throws SQLException {
        String[] parts = parseTableName(tableName);
        String owner = parts[0];
        String table = parts[1];

        // Check if table has supplemental logging enabled
        String sql = "SELECT log_group_name, log_group_type " +
                     "FROM all_log_groups " +
                     "WHERE owner = ? AND table_name = ?";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, owner.toUpperCase());
            pstmt.setString(2, table.toUpperCase());

            try (ResultSet rs = pstmt.executeQuery()) {
                boolean hasSupplementalLog = false;
                while (rs.next()) {
                    hasSupplementalLog = true;
                    String logGroupType = rs.getString("log_group_type");
                    logger.debug("Table {}.{} has supplemental logging type: {}", owner, table, logGroupType);
                }

                if (!hasSupplementalLog) {
                    result.addWarning(String.format(
                            "Table %s.%s does not have supplemental logging enabled. " +
                            "Consider running: ALTER TABLE %s.%s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;",
                            owner, table, owner, table));
                }
            }
        } catch (SQLException e) {
            // If table doesn't exist or permission issue, log warning but don't fail validation
            logger.warn("Unable to check supplemental logging for table {}.{}: {}",
                    owner, table, e.getMessage());
        }
    }

    /**
     * Validate log mining strategy specific requirements
     */
    private static void validateLogMiningStrategy(Connection conn,
                                                  FlinkCDCOracleSourceFactory sourceFactory,
                                                  ValidationResult result) throws SQLException {
        OracleConnectorConfig.LogMiningStrategy strategy = sourceFactory.parseMiningStrategy();
        logger.info("Log mining strategy: {}", strategy);

        switch (strategy) {
            case ONLINE_CATALOG:
                // ONLINE_CATALOG uses current database dictionary
                // Faster but cannot capture DDL operations
                result.addWarning(
                        "Using ONLINE_CATALOG strategy: DDL operations will not be captured. " +
                        "Only DML operations (INSERT/UPDATE/DELETE) will be monitored.");
                break;

            case CATALOG_IN_REDO:
                // CATALOG_IN_REDO extracts dictionary to redo logs
                // Slower but can capture DDL, increases redo log volume
                validateCatalogInRedoRequirements(conn, result);
                result.addWarning(
                        "Using CATALOG_IN_REDO strategy: This will increase redo log volume " +
                        "as data dictionary is written to redo logs.");
                break;

            default:
                logger.warn("Unknown log mining strategy: {}", strategy);
        }
    }

    /**
     * Validate requirements for CATALOG_IN_REDO strategy
     * Database must be open and in ARCHIVELOG mode (already checked above)
     */
    private static void validateCatalogInRedoRequirements(Connection conn, ValidationResult result) throws SQLException {
        // Check if database is open
        String sql = "SELECT open_mode FROM v$database";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                String openMode = rs.getString(1);
                logger.info("Database open_mode: {}", openMode);

                if (!openMode.contains("READ WRITE")) {
                    result.addError(String.format(
                            "For CATALOG_IN_REDO strategy, database must be in READ WRITE mode, current mode: %s",
                            openMode));
                }
            }
        }
    }

    /**
     * Validate user privileges
     * Check if current user has necessary permissions for LogMiner
     */
    private static void validateUserPrivileges(Connection conn, ValidationResult result) throws SQLException {
        // Get current user
        String currentUser;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT USER FROM DUAL")) {
            if (rs.next()) {
                currentUser = rs.getString(1);
                logger.info("Current database user: {}", currentUser);
            } else {
                throw new IllegalStateException("Unable to get current database user");
            }
        }

        // Check required system privileges
        String[] requiredPrivileges = {
                "CREATE SESSION",
                "LOGMINING",
                "SELECT ANY TRANSACTION",
                "SELECT ANY TABLE"
        };

        List<String> missingPrivileges = new ArrayList<>();

        for (String privilege : requiredPrivileges) {
            if (!hasPrivilege(conn, currentUser, privilege)) {
                missingPrivileges.add(privilege);
            }
        }

        if (!missingPrivileges.isEmpty()) {
            result.addError(String.format(
                    "User %s is missing required privileges: %s. " +
                    "Please grant these privileges to the user.",
                    currentUser, String.join(", ", missingPrivileges)));
        }

        // Check access to LogMiner views
        String[] requiredViews = {
                "V$DATABASE",
                "V$LOGMNR_CONTENTS",
                "V$LOGMNR_LOGS",
                "V$ARCHIVED_LOG"
        };

        for (String view : requiredViews) {
            if (!canAccessView(conn, view)) {
                result.addWarning(String.format(
                        "User %s may not have access to view %s, which is required for LogMiner",
                        currentUser, view));
            }
        }
    }

    /**
     * Check if user has specific privilege
     */
    private static boolean hasPrivilege(Connection conn, String username, String privilege) {
        String sql = "SELECT COUNT(*) FROM user_sys_privs WHERE privilege = ?";

        try (java.sql.PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, privilege);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            logger.warn("Unable to check privilege {} for user {}: {}",
                    privilege, username, e.getMessage());
        }

        return false;
    }

    /**
     * Check if user can access a specific view
     */
    private static boolean canAccessView(Connection conn, String viewName) {
        String sql = "SELECT 1 FROM " + viewName + " WHERE ROWNUM = 1";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return true; // If query succeeds, user has access
        } catch (SQLException e) {
            logger.debug("Cannot access view {}: {}", viewName, e.getMessage());
            return false;
        }
    }

    /**
     * Parse table name, return [owner, table]
     * If owner is not specified, use current user
     */
    private static String[] parseTableName(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        String[] parts = tableName.split("\\.");
        if (parts.length == 1) {
            // No schema specified, need to get current user
            return new String[]{"", parts[0]};
        } else if (parts.length == 2) {
            return new String[]{parts[0], parts[1]};
        } else {
            throw new IllegalArgumentException("Invalid table name format: " + tableName);
        }
    }

    /**
     * Check if version is supported
     */
    private static boolean isVersionSupported(String version) {
        if (StringUtils.isEmpty(version)) {
            return false;
        }

        try {
            // Oracle version format: "11.2.0.4.0", "12.1.0.2.0", "19.3.0.0.0", etc.
            String[] parts = version.split("\\.");

            if (parts.length > 0) {
                int majorVersion = Integer.parseInt(parts[0]);
                return majorVersion >= MIN_MAJOR_VERSION;
            }
        } catch (NumberFormatException e) {
            logger.warn("Unable to parse Oracle version: {}", version);
        }

        return false;
    }

    /**
     * Validation result class
     */
    public static class ValidationResult {
        private final List<String> errors = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        public void addError(String error) {
            if (StringUtils.isNotEmpty(error)) {
                errors.add(error);
                logger.error("Oracle CDC validation error: {}", error);
            }
        }

        public void addWarning(String warning) {
            if (StringUtils.isNotEmpty(warning)) {
                warnings.add(warning);
                logger.warn("Oracle CDC validation warning: {}", warning);
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
                sb.append("Errors:\n");
                errors.forEach(e -> sb.append("  - ").append(e).append("\n"));
            }
            if (!warnings.isEmpty()) {
                sb.append("Warnings:\n");
                warnings.forEach(w -> sb.append("  - ").append(w).append("\n"));
            }
            return sb.toString();
        }
    }
}

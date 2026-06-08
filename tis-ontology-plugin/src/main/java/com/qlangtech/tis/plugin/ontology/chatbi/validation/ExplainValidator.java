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
package com.qlangtech.tis.plugin.ontology.chatbi.validation;

import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.graphrag.RetrievalResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * EXPLAIN 校验器（§5.2 T4）。
 * <p>
 * 投递 EXPLAIN 到数据库，捕获语义错误（如函数签名不匹配、类型错误、聚合错误等）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class ExplainValidator implements SqlValidator {

    private static final Logger logger = LoggerFactory.getLogger(ExplainValidator.class);

    private final String domain;
    private final int explainTimeoutSeconds;

    public ExplainValidator(String domain) {
        this(domain, 5);
    }

    public ExplainValidator(String domain, int explainTimeoutSeconds) {
        if (domain == null || domain.isEmpty()) {
            throw new IllegalArgumentException("domain cannot be null or empty");
        }
        this.domain = domain;
        this.explainTimeoutSeconds = explainTimeoutSeconds;
    }

    @Override
    public ValidationResult validate(String sql, RetrievalResult context) {
        if (StringUtils.isBlank(sql)) {
            return ValidationResult.fail("SQL is empty");
        }
        try {
            // 加载数据源
            if (CollectionUtils.isEmpty(context.objectTypes())) {
                throw new IllegalStateException("objectTypes can not be empty");
            }
            DataSourceFactory dataSource = null;
            for (String f : context.objectTypes()) {
                OntologyObjectType objType = com.qlangtech.tis.plugin.ontology.Ontology.loadObjectTypeDetail(domain, f);
                dataSource = Objects.requireNonNull(objType, "objType can not be null fetch by:" + f).getDataSourceBinding().getDataSource();
                break;
            }

            //.get DataSourceFactory.load(domain);
            if (dataSource == null) {
                logger.warn("DataSource not found for domain: {}", domain);
                return ValidationResult.fail("DataSource not found: " + domain);
            }

            // 执行 EXPLAIN 校验
            ValidationResultHolder resultHolder = new ValidationResultHolder();
            dataSource.visitFirstConnection(conn -> {
                resultHolder.result = executeExplain(conn, sql);
            });

            return resultHolder.result != null ? resultHolder.result : ValidationResult.ok();

        } catch (Exception e) {
            logger.error("Failed to validate SQL with EXPLAIN for domain: {}", domain, e);
            return ValidationResult.fail("EXPLAIN validation failed", new Exception(sql, e));
        }
    }

    private ValidationResult executeExplain(JDBCConnection conn, String sql) {
        Statement stmt = null;
        try {
            stmt = conn.getConnection().createStatement();
            stmt.setQueryTimeout(explainTimeoutSeconds);

            // 执行 EXPLAIN（不实际运行查询）
            String explainSql = "EXPLAIN " + sql;
            logger.debug("Executing EXPLAIN: {}", explainSql);

            stmt.execute(explainSql);

            // EXPLAIN 执行成功，说明 SQL 语法和语义都正确
            return ValidationResult.ok();

        } catch (SQLException e) {
            // 捕获 SQL 错误（函数签名不匹配、类型错误等）
            String errorMsg = e.getMessage();
            logger.warn("EXPLAIN validation failed: {}", errorMsg);

            // 提取关键错误信息
            String simplifiedError = simplifyErrorMessage(errorMsg);
            return ValidationResult.fail("SQL validation failed: " + simplifiedError);

        } catch (Exception e) {
            logger.error("Unexpected error during EXPLAIN validation", e);
            return ValidationResult.fail("Unexpected validation error: " + e.getMessage());

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.warn("Failed to close statement", e);
                }
            }
        }
    }

    /**
     * 简化错误消息，提取关键信息给 LLM 参考
     */
    private String simplifyErrorMessage(String errorMsg) {
        if (errorMsg == null) {
            return "Unknown error";
        }

        // 提取 Doris 错误消息的关键部分
        // 例如：errCode = 2, detailMessage = No matching function with signature: date_diff(...)
        if (errorMsg.contains("detailMessage")) {
            int startIdx = errorMsg.indexOf("detailMessage");
            String detail = errorMsg.substring(startIdx);
            // 移除前缀
            detail = detail.replace("detailMessage = ", "").trim();
            return detail;
        }

        // 如果消息太长，截断
        if (errorMsg.length() > 200) {
            return errorMsg.substring(0, 200) + "...";
        }

        return errorMsg;
    }

    /**
     * 用于在 lambda 中传递结果
     */
    private static class ValidationResultHolder {
        ValidationResult result;
    }
}

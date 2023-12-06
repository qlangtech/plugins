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

import org.apache.flink.test.util.AbstractTestBase;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/**
 * Basic class for testing PostgresSQL source, this contains a PostgreSQL container which enables
 * binlog.
 */
public abstract class PostgresTestBase extends AbstractTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    public static final String DEFAULT_DB = "postgres";
    private static final DockerImageName PG_IMAGE =
            //  DockerImageName.parse("debezium/postgres:9.6")
            DockerImageName.parse("postgres:latest")
                    .asCompatibleSubstituteFor("postgres");
    // config_file=/etc/postgresql/postgresql.conf
    //document: https://hub.docker.com/_/postgres
    protected static final PostgreSQLContainer<?> POSTGERS_CONTAINER =
            new PostgreSQLContainer<>(PG_IMAGE)
                    .withClasspathResourceMapping( //
                            "postgresql/postgresql.conf" //
                            , "/etc/postgresql/postgresql.conf" //
                            , BindMode.READ_WRITE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG) {
                        @Override
                        public void accept(OutputFrame outputFrame) {
                            OutputFrame.OutputType outputType = outputFrame.getType();
                            String utf8String = outputFrame.getUtf8String();
                            System.out.println(utf8String);
                            super.accept(outputFrame);
                        }
                    }).withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "config_file=/etc/postgresql/postgresql.conf");


    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGERS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGERS_CONTAINER.getJdbcUrl(),
                POSTGERS_CONTAINER.getUsername(),
                POSTGERS_CONTAINER.getPassword());
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the
     * connection.
     */
    protected void initializePostgresTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = PostgresTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
             Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            boolean success;
            for (String stmt : statements) {
                success = statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

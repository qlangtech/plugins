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
package com.qlangtech.tis.dump.hive;

import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.config.authtoken.impl.OffUserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.dump.IExecLiveLogParser;
import com.qlangtech.tis.dump.spark.SparkExecLiveLogParser;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus.JoinTaskStatus;
import com.qlangtech.tis.hive.Hms;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DelegatingStatement;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年10月7日 下午4:20:38
 */
public class HiveDBUtils {

    private String hiveJdbcUrl;

    // = new BasicDataSource();
    private final BasicDataSource hiveDatasource;

    private static final Logger log = LoggerFactory.getLogger(HiveDBUtils.class);

    private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 500;


    // =
    private static final ExecutorService exec;

    static {
        exec = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>()) {

            protected void afterExecute(Runnable r, Throwable t) {
                if (t != null) {
                    log.error(t.getMessage(), t);
                }
            }
        };
    }

    private static HiveDBUtils hiveHelper;


    public static HiveDBUtils getInstance(String hiveHost, String defaultDbName) {
        return getInstance(hiveHost, defaultDbName, new OffUserToken());
    }

    public static HiveDBUtils getInstance(String hiveHost, String defaultDbName, UserToken userToken) {
        if (hiveHelper == null) {
            synchronized (HiveDBUtils.class) {
                if (hiveHelper == null) {
                    hiveHelper = new HiveDBUtils(hiveHost, defaultDbName, userToken);
                }
            }
        }
        return hiveHelper;
    }

//    public static class UserToken {
//        public final String userName;
//        public final String password;
//
//        public UserToken(String userName, String password) {
//            this.userName = userName;
//            this.password = password;
//        }
//    }

    private HiveDBUtils(String hiveHost, String defaultDbName, UserToken userToken) {
        this.hiveDatasource = createDatasource(hiveHost, defaultDbName, userToken);
    }

    public static String createHiveJdbcUrl(String hiveHost, String defaultDbName) {
        StringBuffer jdbcUrl = new StringBuffer(IHiveConnGetter.HIVE2_JDBC_SCHEMA + hiveHost + "/" + defaultDbName);
        return jdbcUrl.toString();
    }


    // private static final String hiveHost;
    private BasicDataSource createDatasource(String hiveHost, String defaultDbName, UserToken userToken) {
        if (StringUtils.isEmpty(hiveHost)) {
            throw new IllegalArgumentException("param 'hiveHost' can not be null");
        }
        if (StringUtils.isEmpty(defaultDbName)) {
            throw new IllegalArgumentException("param 'defaultDbName' can not be null");
        }
        String jdbcUrl = createHiveJdbcUrl(hiveHost, defaultDbName);
        BasicDataSource hiveDatasource = new BasicDataSource() {
            @Override
            protected ConnectionFactory createConnectionFactory() throws SQLException {
                return new ConnectionFactory() {
                    @Override
                    public Connection createConnection() throws SQLException {
                        try {
                            return Hms.createConnection(jdbcUrl, userToken).getConnection();
                        } catch (SQLException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new SQLException(e);
                        }
                    }
                };
            }
        };
        hiveDatasource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        hiveDatasource.setDriverClassLoader(this.getClass().getClassLoader());

        Assert.assertNotNull("driverClassLoader can not be null", hiveDatasource.getDriverClassLoader());
        // hiveDatasource.setUsername("hive");
        // 这个配置是在每次操作之后连接没有有效关闭时候，定时会执行清理操作，把没有及时归还的，將2.5小時還沒有歸還pool的連接直接關閉掉
        hiveDatasource.setMaxActive(-1);
        hiveDatasource.setRemoveAbandoned(true);
        hiveDatasource.setLogAbandoned(true);
        hiveDatasource.setRemoveAbandonedTimeout(300 * 30);

        if (StringUtils.isBlank(hiveHost)) {
            throw new IllegalStateException("hivehost can not be null");
        }
        // 测试空闲的连接是否有效
        hiveDatasource.setTestWhileIdle(true);
        if (StringUtils.isBlank(hiveHost)) {
            throw new IllegalStateException("hivehost can not be null");
        }
        // String hiveJdbcUrl = "jdbc:hive2://" + hiveHost + "/tis";
        hiveJdbcUrl = jdbcUrl;
        hiveDatasource.setUrl(hiveJdbcUrl);
        log.info("hiveJdbcUrl:" + hiveJdbcUrl);
        return hiveDatasource;
    }

    public JDBCConnection createConnection() {
        return createConnection(0);
    }

    public JDBCConnection createConnection(int retry) {
        JDBCConnection conn = null;
        try {
            conn = new JDBCConnection(hiveDatasource.getConnection(), hiveJdbcUrl);
            executeNoLog(conn, "set hive.exec.dynamic.partition.mode=nonstrict");
            return conn;
        } catch (Exception e) {
            if (retry < 5) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                }
                try {
                    if (conn != null) {
                        close(conn);
                    }
                } catch (Throwable e1) {
                }
                log.warn("retry:" + retry, e);
                return createConnection(++retry);
            } else {
                throw new IllegalStateException("retry:" + retry + ",hivehost:" + hiveJdbcUrl, e);
            }
        }
    }

    public void close(JDBCConnection conn) {
        try {
            conn.close();
        } catch (Throwable e) {
        }
    }

    public static boolean execute(JDBCConnection conn, String sql, IJoinTaskStatus joinTaskStatus) throws SQLException {
        return execute(conn, sql, true, /* listenLog */          joinTaskStatus);
    }

    public static boolean execute(JDBCConnection conn, String sql) throws SQLException {
        return execute(conn, sql, new JoinTaskStatus("dump"));
    }

    public static boolean executeNoLog(JDBCConnection conn, String sql) throws SQLException {
        return execute(conn, sql, false, /* listenLog */
                new JoinTaskStatus("dump"));
    }

    /**
     * 执行一个sql语句
     *
     * @param sql
     * @return
     * @throws Exception
     */
    private static boolean execute(JDBCConnection conn, String sql, boolean listenLog, IJoinTaskStatus joinTaskStatus) throws SQLException {
        synchronized (HiveDBUtils.class) {
            try (Statement stmt = conn.getConnection().createStatement()) {
                // Future<?> f = null;// exec.submit(createLogRunnable(stmt));
                try {
                    if (listenLog) {
                        exec.execute(createLogRunnable(stmt, joinTaskStatus));
                    }
                    return stmt.execute(sql);
                } catch (SQLException e) {
                    joinTaskStatus.setFaild(true);
                    throw new RuntimeException(sql, e);
                } finally {
                    joinTaskStatus.setComplete(true);
                    try {
                        if (listenLog) {
                            // f.cancel(true);
                        }
                    } catch (Throwable e) {
                    }
                }
            }
        }
    }

    private static Runnable createLogRunnable(Statement statement, IJoinTaskStatus joinTaskStatus) {
        final String collection = MDC.get(JobParams.KEY_COLLECTION);
//        if (StringUtils.isNotEmpty(collection)) {
//            throw new IllegalArgumentException("collection has not been set in MDC context");
//        }
        final Integer taskId;
        try {
            taskId = Integer.parseInt(MDC.get(JobCommon.KEY_TASK_ID));
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("have not set " + JobCommon.KEY_TASK_ID + " in MDC context", e);
        }
        HiveStatement hStatement = null;
        if (statement instanceof HiveStatement) {
            hStatement = (HiveStatement) statement;
        } else if (statement instanceof org.apache.commons.dbcp.DelegatingStatement) {
            hStatement = (HiveStatement) ((DelegatingStatement) statement).getInnermostDelegate();
        } else {
            log.debug("The statement instance is not HiveStatement type: " + statement.getClass());
            return () -> {
            };
        }
        final HiveStatement hiveStatement = hStatement;

        // TODO 这里将来可以按照配置切换
        // final IExecLiveLogParser hiveLiveLogParser = new HiveExecLiveLogParser(joinTaskStatus);
        final IExecLiveLogParser hiveLiveLogParser = new SparkExecLiveLogParser(joinTaskStatus);
        Runnable runnable = new Runnable() {

            @Override
            public void run() {

                JobCommon.setMDC((taskId), collection);
                // getStatementId(hiveStatement);
                while (hiveStatement.hasMoreLogs()) {
                    try {
                        for (String logmsg : hiveStatement.getQueryLog()) {
                            if (!hiveLiveLogParser.isExecOver()) {
                                log.info(logmsg);
                                hiveLiveLogParser.process(logmsg);
                            }
                        }
                        try {
                            Thread.sleep(DEFAULT_QUERY_PROGRESS_INTERVAL);
                        } catch (Throwable e) {
                            return;
                        }
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                        return;
                    }
                }
            }
        };
        return runnable;
    }

//    private static void getStatementId(HiveStatement hiveStatement) {
//        // private TOperationHandle stmtHandle = null;
//        TOperationHandle stmtHandle = null;
//        try {
//            int i = 0;
//            while (stmtHandle == null && i++ < 4) {
//                Field stmtHandleField = HiveStatement.class.getDeclaredField("stmtHandle");
//                stmtHandleField.setAccessible(true);
//                stmtHandle = (TOperationHandle) stmtHandleField.get(hiveStatement);
//                if (stmtHandle == null) {
//                    Thread.sleep(1000);
//                }
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        Objects.requireNonNull(stmtHandle, "stmtHandle can not be null");
//       new String( stmtHandle.getOperationId().getGuid());
//    }


    public static void main(String[] args) throws Exception {

        HiveDBUtils.class.getResource("/org/apache/hive/service/cli/operation/SQLOperation.class");

        HiveDBUtils dbUtils = HiveDBUtils.getInstance("192.168.28.200", "tis");

        JDBCConnection con = dbUtils.createConnection();

        // // Connection con = DriverManager.getConnection(
        // // "jdbc:hive://10.1.6.211:10000/tis", "", "");
        // System.out.println("start create connection");
        // // Connection con = DriverManager.getConnection(
        // // "jdbc:hive2://hadoop6:10001/tis", "", "");
        // System.out.println("create conn");
        Statement stmt = con.getConnection().createStatement();
        //
        ResultSet result = stmt.executeQuery("select 1");
        if (result.next()) {
            System.out.println(result.getInt(1));
        }
        //
        // while (result.next()) {
        // System.out.println("cols:" + result.getString(1));
        // }
        // String tableName = "testHiveDriverTable";
        // // stmt.executeQuery("drop table " + tableName);
        //
        // stmt.execute("drop table " + tableName);
        // ResultSet res = null;
        // stmt.execute("create table " + tableName +
        // " (key int, value string)");
        // // show tables
        // String sql = "show tables '" + tableName + "'";
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // if (res.next()) {
        // System.out.println(res.getString(1));
        // }
        // // describe table
        // sql = "describe " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // while (res.next()) {
        // System.out.println(res.getString(1) + "\t" + res.getString(2));
        // }
        //
        // // load data into table
        // // NOTE: filepath has to be local to the hive server
        // // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per
        // line
        // String filepath = "/tmp/a.txt";
        // sql = "load data local inpath '" + filepath + "' into table "
        // + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        //
        // // select * query
        // sql = "select * from " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        // while (res.next()) {
        // System.out.println(String.valueOf(res.getInt(1)) + "\t"
        // + res.getString(2));
        // }
        //
        // // regular hive query
        // String sql = "select count(1) from " + tableName;
        // HiveDBUtils hiveHelper = new HiveDBUtils();
        // String sql = IOUtils.toString(Thread.currentThread()
        // .getContextClassLoader()
        // .getResourceAsStream("create_tmp_order_instance.txt"));
        // System.out.println("Running: " + sql);
        // // ResultSet res =
        // stmt.execute(sql);
        // stmt.close();
        //
        // System.out.println("another conn");
        // hiveHelper.query(con, "show tables", new ResultProcess() {
        // @Override
        // public void callback(ResultSet result) throws Exception {
        // System.out.println(result.getString(1));
        // }
        // });
        // System.out.println("===============================================");
        // System.out.println("same connection");
        // stmt = con.createStatement();
        // ResultSet res = stmt.executeQuery("show tables");
        // while (res.next()) {
        // System.out.println(res.getString(1));
        // }
        // stmt.close();
        // con.close();
    }
}

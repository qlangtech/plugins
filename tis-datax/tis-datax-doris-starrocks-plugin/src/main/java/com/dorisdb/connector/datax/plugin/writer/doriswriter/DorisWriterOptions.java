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

package com.dorisdb.connector.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DorisWriterOptions implements Serializable {

    private static final long serialVersionUID = 1l;
    private static final long KILO_BYTES_SCALE = 1024l;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final int MAX_RETRIES = 1;
    private static final int BATCH_ROWS = 500000;
    private static final long BATCH_BYTES = 90 * MEGA_BYTES_SCALE;

    private static final String KEY_LOAD_PROPS_FORMAT = "format";

    public enum StreamLoadFormat {
        CSV, JSON;
    }

    private static final String KEY_USERNAME = "username";
    private static final String KEY_PASSWORD = "password";
    private static final String KEY_DATABASE = "database";
    private static final String KEY_TABLE = "table";
    private static final String KEY_COLUMN = "column";
    private static final String KEY_PRE_SQL = "preSql";
    private static final String KEY_POST_SQL = "postSql";
    private static final String KEY_JDBC_URL = "jdbcUrl";
    private static final String KEY_MAX_BATCH_ROWS = "maxBatchRows";
    private static final String KEY_MAX_BATCH_SIZE = "maxBatchSize";
    private static final String KEY_LOAD_URL = "loadUrl";
    private static final String KEY_FLUSH_QUEUE_LENGTH = "flushQueueLength";
    private static final String KEY_LOAD_PROPS = "loadProps";

    private final Configuration options;

    public DorisWriterOptions(Configuration options) {
        this.options = options;
    }

    public void doPretreatment() {
        validateRequired();
        validateStreamLoadUrl();
    }

    public String getJdbcUrl() {
        return options.getString(KEY_JDBC_URL);
    }

    public String getDatabase() {
        return options.getString(KEY_DATABASE);
    }

    public String getTable() {
        return options.getString(KEY_TABLE);
    }

    public String getUsername() {
        return options.getString(KEY_USERNAME);
    }

    public String getPassword() {
        return options.getString(KEY_PASSWORD);
    }

    public List<String> getLoadUrlList() {
        return options.getList(KEY_LOAD_URL, String.class);
    }

    public List<String> getColumns() {
        return options.getList(KEY_COLUMN, String.class).stream().map(str -> str.replace("`", "")).collect(Collectors.toList());
    }

    public List<String> getPreSqlList() {
        return options.getList(KEY_PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return options.getList(KEY_POST_SQL, String.class);
    }

    public Map<String, Object> getLoadProps() {
        Map<String, Object> props = options.getMap(KEY_LOAD_PROPS);
        return props == null ? Collections.emptyMap() : props;
    }

    public int getMaxRetries() {
        return MAX_RETRIES;
    }

    public int getBatchRows() {
        Integer rows = options.getInt(KEY_MAX_BATCH_ROWS);
        return null == rows ? BATCH_ROWS : rows;
    }

    public long getBatchSize() {
        Long size = options.getLong(KEY_MAX_BATCH_SIZE);
        return null == size ? BATCH_BYTES : size;
    }

    public int getFlushQueueLength() {
        Integer len = options.getInt(KEY_FLUSH_QUEUE_LENGTH);
        return null == len ? 1 : len;
    }

    public StreamLoadFormat getStreamLoadFormat() {
        Map<String, Object> loadProps = getLoadProps();
        if (null == loadProps) {
            return StreamLoadFormat.CSV;
        }
        if (loadProps.containsKey(KEY_LOAD_PROPS_FORMAT)
                && StreamLoadFormat.JSON.name().equalsIgnoreCase(String.valueOf(loadProps.get(KEY_LOAD_PROPS_FORMAT)))) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
    }

    private void validateStreamLoadUrl() {
        List<String> urlList = getLoadUrlList();
        for (String host : urlList) {
            if (host.split(":").length < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "loadUrl的格式不正确，请输入 `fe_ip:fe_http_ip;fe_ip:fe_http_ip`。");
            }
        }
    }

    private void validateRequired() {
        final String[] requiredOptionKeys = new String[]{
                KEY_USERNAME,
                //KEY_PASSWORD,
                KEY_DATABASE,
                KEY_TABLE,
                KEY_COLUMN,
                KEY_LOAD_URL
        };
        for (String optionKey : requiredOptionKeys) {
            options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }
}

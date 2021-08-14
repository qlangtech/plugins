/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax.common;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-08-06 22:28
 **/
public class RdbmsWriter {
    private static final Logger logger = LoggerFactory.getLogger(RdbmsWriter.class);

    /**
     * 初始化表RDBMS的表，如果表不存在就创建表
     *
     * @param cfg
     * @throws Exception
     */
    public static void initWriterTable(Configuration cfg) throws Exception {
        String dataXName = cfg.getNecessaryValue(DataxUtils.DATAX_NAME, RdbmsWriterErrorCode.REQUIRED_DATAX_PARAM_ERROR);
        BasicDataXRdbmsWriter<BasicDataSourceFactory> dataXWriter
                = (BasicDataXRdbmsWriter<BasicDataSourceFactory>) DataxWriter.load(null, dataXName);

        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null,dataXName:" + dataXName);
        boolean autoCreateTable = dataXWriter.autoCreateTable;
        if (autoCreateTable) {
            DataxProcessor processor = DataxProcessor.load(null, dataXName);
            String tableName = cfg.getNecessaryValue(Constant.CONN_MARK + "[0]." + Key.TABLE + "[0]", RdbmsWriterErrorCode.REQUIRED_TABLE_NAME_PARAM_ERROR);
            File createDDL = new File(processor.getDataxCreateDDLDir(null), tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            if (!createDDL.exists()) {
                throw new IllegalStateException("create table script is not exist:" + createDDL.getAbsolutePath());
            }

            BasicDataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
            List<Object> connections = cfg.getList(Constant.CONN_MARK, Object.class);
            String createScript = FileUtils.readFileToString(createDDL, TisUTF8.get());
            for (int i = 0, len = connections.size(); i < len; i++) {
                Configuration connConf = Configuration.from(String.valueOf(connections.get(i)));
                String jdbcUrl = connConf.getString(Key.JDBC_URL);
                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
                    List<String> tabs = Lists.newArrayList();
                    dsFactory.refectTableInDB(tabs, conn);

                    if (!tabs.contains(tableName)) {
                        // 表不存在
                        try (Statement statement = conn.createStatement()) {
                            logger.info("create table:{}\n   script:{}", tableName, createScript);
                            statement.execute(createScript);
                        }
                    }
                }
            }
        }
    }
}

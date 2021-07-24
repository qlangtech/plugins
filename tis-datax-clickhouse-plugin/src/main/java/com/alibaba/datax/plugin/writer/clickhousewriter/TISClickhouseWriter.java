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

package com.alibaba.datax.plugin.writer.clickhousewriter;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.DataXClickhouseWriter;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * 这个扩展是想实现Clickhouse的自动建表
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-10 09:10
 * @see com.qlangtech.tis.plugin.datax.DataXClickhouseWriter
 **/
public class TISClickhouseWriter extends com.alibaba.datax.plugin.writer.clickhousewriter.ClickhouseWriter {
    private static final Logger logger = LoggerFactory.getLogger(TISClickhouseWriter.class);

    public static class Job extends ClickhouseWriter.Job {
        @Override
        public void prepare() {
            String dataXName = this.originalConfig.getNecessaryValue(DataxUtils.DATAX_NAME, ClickhouseWriterErrorCode.REQUIRED_DATAX_PARAM_ERROR);
            DataXClickhouseWriter dataXWriter = (DataXClickhouseWriter) DataxWriter.getPluginStore(null, dataXName).getPlugin();


            Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
            boolean autoCreateTable = dataXWriter.autoCreateTable;
            if (autoCreateTable) {
                DataxProcessor processor = DataxProcessor.load(null, dataXName);
                String tableName = originalConfig.getNecessaryValue(Constant.CONN_MARK + "[0]." + Key.TABLE + "[0]", ClickhouseWriterErrorCode.REQUIRED_TABLE_NAME_PARAM_ERROR);
                File createDDL = new File(processor.getDataxCreateDDLDir(null), tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
                if (!createDDL.exists()) {
                    throw new IllegalStateException("create table script is not exist:" + createDDL.getAbsolutePath());
                }

                ClickHouseDataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
                try {
                    List<String> tables = dsFactory.getTablesInDB();
                    // 先判断表是否存在
                    if (!tables.contains(tableName)) {
                        // 表不存在
                        Connection conn = null;
                        Statement statement = null;
                        try {
                            conn = dsFactory.getConnection(dsFactory.getJdbcUrl());
                            statement = conn.createStatement();
                            String script = FileUtils.readFileToString(createDDL, TisUTF8.get());
                            logger.info("create table:{}\n   script:{}", tableName, script);
                            statement.execute(script);
                        } finally {
                            DBUtil.closeDBResources(null, statement, conn);
                        }

                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            super.prepare();
        }
    }

    public static class Task extends ClickhouseWriter.Task {

    }
}

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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-18 17:55
 **/
public class InitWriterTable {

    private static final Logger logger = LoggerFactory.getLogger(InitWriterTable.class);

    /**
     * 初始化表RDBMS的表，如果表不存在就创建表
     *
     * @param
     * @throws Exception
     */
    public static void process(String dataXName, String tableName, List<String> jdbcUrls) throws Exception {
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("param dataXName can not be null");
        }
        BasicDataXRdbmsWriter<BasicDataSourceFactory> dataXWriter
                = (BasicDataXRdbmsWriter<BasicDataSourceFactory>) DataxWriter.load(null, dataXName);

        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null,dataXName:" + dataXName);
        boolean autoCreateTable = dataXWriter.autoCreateTable;
        if (autoCreateTable) {
            DataxProcessor processor = DataxProcessor.load(null, dataXName);

            File createDDL = new File(processor.getDataxCreateDDLDir(null)
                    , tableName + IDataxProcessor.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            if (!createDDL.exists()) {
                throw new IllegalStateException("create table script is not exist:" + createDDL.getAbsolutePath());
            }

            BasicDataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
            String createScript = FileUtils.readFileToString(createDDL, TisUTF8.get());
            for (String jdbcUrl : jdbcUrls) {
                try (Connection conn = dsFactory.getConnection(jdbcUrl)) {
                    List<String> tabs = Lists.newArrayList();
                    dsFactory.refectTableInDB(tabs, conn);
                    if (!tabs.contains(tableName)) {
                        // 表不存在
                        try (Statement statement = conn.createStatement()) {
                            logger.info("create table:{}\n   script:{}", tableName, createScript);
                            statement.execute(createScript);
                        }
                    } else {
                        logger.info("table:{} already exist ,skip the create table step", tableName);
                    }
                }
            }
        }
    }
}

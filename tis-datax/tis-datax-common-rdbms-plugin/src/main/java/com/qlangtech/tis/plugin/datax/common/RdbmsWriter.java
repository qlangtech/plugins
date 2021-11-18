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
import com.qlangtech.tis.offline.DataxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-08-06 22:28
 **/
public class RdbmsWriter {
    private static final Logger logger = LoggerFactory.getLogger(RdbmsWriter.class);


    /**
     * 适用于这种类型的配置
     * "connection":[
     * {
     * "jdbcUrl":"jdbc:clickhouse://192.168.28.201:8123/tis",
     * "table":[
     * "instancedetail"
     * ]
     * }
     * ],
     *
     * @param cfg
     * @throws Exception
     */
    public static void initWriterTable(Configuration cfg) throws Exception {
        String dataXName = cfg.getNecessaryValue(DataxUtils.DATAX_NAME, RdbmsWriterErrorCode.REQUIRED_DATAX_PARAM_ERROR);

        String tableName = cfg.getNecessaryValue(Constant.CONN_MARK + "[0]." + Key.TABLE + "[0]"
                , RdbmsWriterErrorCode.REQUIRED_TABLE_NAME_PARAM_ERROR);
        List<String> jdbcUrls = Lists.newArrayList();
        List<Object> connections = cfg.getList(Constant.CONN_MARK, Object.class);
        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(String.valueOf(connections.get(i)));
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            jdbcUrls.add(jdbcUrl);
        }

        InitWriterTable.process(dataXName, tableName, jdbcUrls);
    }


}

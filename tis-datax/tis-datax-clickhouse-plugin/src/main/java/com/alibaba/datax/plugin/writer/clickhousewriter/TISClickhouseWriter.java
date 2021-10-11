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

import com.alibaba.datax.common.util.Configuration;
import com.qlangtech.tis.plugin.datax.common.RdbmsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        public void init() {
            Configuration cfg = super.getPluginJobConf();
            // 判断表是否存在，如果不存在则创建表
            try {
                RdbmsWriter.initWriterTable(cfg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            super.init();
        }

//        @Override
//        public void prepare() {
//            super.prepare();
//        }

    }

    public static class Task extends ClickhouseWriter.Task {

    }
}

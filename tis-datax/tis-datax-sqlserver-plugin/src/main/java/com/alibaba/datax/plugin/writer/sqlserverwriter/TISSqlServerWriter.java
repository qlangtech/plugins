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

package com.alibaba.datax.plugin.writer.sqlserverwriter;

import com.alibaba.datax.common.util.Configuration;
import com.qlangtech.tis.plugin.datax.common.RdbmsWriter;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-18 14:40
 **/
public class TISSqlServerWriter extends SqlServerWriter {

    public static class Job extends SqlServerWriter.Job {

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
    }

    public static class Task extends SqlServerWriter.Task{

    }
}

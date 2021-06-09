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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 09:09
 **/
public class TisDataXTiDBWriter extends Reader {
    public static class Job extends Reader.Job {
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return null;
        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {
        @Override
        public void startRead(RecordSender recordSender) {

        }

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }
    }
}

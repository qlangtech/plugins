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

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:48
 **/
public class DataXSparkWriter extends DataXHiveWriter {
    private static final String DATAX_NAME = "Spark";

    public MREngine getEngineType() {
        return MREngine.SPARK;
    }

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new HiveDataXContext("tissparkwriter", tableMap, this.dataXName);
    }

    @TISExtension()
    public static class DataXSparkWriterDescriptor extends DataXHiveWriter.DefaultDescriptor {

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}

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
import com.qlangtech.tis.plugin.datax.common.RdbmsWriterContext;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:58
 **/
public class DorisWriterContext extends RdbmsWriterContext<DataXDorisWriter, DorisSourceFactory> {

    public DorisWriterContext(DataXDorisWriter writer, IDataxProcessor.TableMap tabMapper) {
        super(writer, tabMapper);
    }

    public String getDataXName() {
        return plugin.dataXName;
    }

    public String getDatabase() {
        return this.dsFactory.dbName;
    }

    public String getLoadUrl() {
        return this.dsFactory.loadUrl;
    }

    public boolean isContainLoadProps() {
        return StringUtils.isNotBlank(this.plugin.loadProps);
    }

    public String getLoadProps() {
        return this.plugin.loadProps;
    }

    public boolean isContainMaxBatchRows() {
        return this.plugin.maxBatchRows != null;
    }

    public Integer getMaxBatchRows() {
        return this.plugin.maxBatchRows;
    }

    public boolean isContainMaxBatchSize() {
        return this.plugin.batchSize != null;
    }

    public Integer getMaxBatchSize() {
        return this.plugin.batchSize;
    }

    protected String colEscapeChar() {
        return StringUtils.EMPTY;
    }


}
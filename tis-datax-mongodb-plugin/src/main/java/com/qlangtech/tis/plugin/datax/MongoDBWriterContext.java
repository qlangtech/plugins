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

import com.qlangtech.tis.datax.IDataxContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-20 14:01
 **/
public class MongoDBWriterContext extends BasicMongoDBContext implements IDataxContext {
    private final DataXMongodbWriter writer;

    public MongoDBWriterContext(DataXMongodbWriter writer) {
        super(writer.getDsFactory());
        this.writer = writer;
    }

    public String getCollectionName() {
        return this.writer.collectionName;
    }

    public String getColumn() {
        return this.writer.column;
    }


    public boolean isContainUpsertInfo() {
        return StringUtils.isNotBlank(this.writer.upsertInfo);
    }

    public String getUpsertInfo() {
        return this.writer.upsertInfo;
    }
}

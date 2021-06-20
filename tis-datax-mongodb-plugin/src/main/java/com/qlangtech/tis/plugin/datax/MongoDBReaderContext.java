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

import com.qlangtech.tis.datax.IDataxReaderContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 14:53
 **/
public class MongoDBReaderContext extends BasicMongoDBContext implements IDataxReaderContext {
    private final DataXMongodbReader mongodbReader;
    private final String taskName;

    public MongoDBReaderContext(String taskName, DataXMongodbReader mongodbReader) {
        super(mongodbReader.getDsFactory());
        this.mongodbReader = mongodbReader;
        this.taskName = taskName;
    }


    public String getCollectionName() {
        return mongodbReader.collectionName;
    }

    public String getColumn() {
        return this.mongodbReader.column;
    }

    public boolean isContainQuery() {
        return StringUtils.isNotEmpty(this.mongodbReader.query);
    }

    public String getQuery() {
        return this.mongodbReader.query;
    }

    @Override
    public String getTaskName() {
        return this.taskName;
    }

    @Override
    public String getSourceEntityName() {
        return DataXMongodbReader.DATAX_NAME;
    }
}

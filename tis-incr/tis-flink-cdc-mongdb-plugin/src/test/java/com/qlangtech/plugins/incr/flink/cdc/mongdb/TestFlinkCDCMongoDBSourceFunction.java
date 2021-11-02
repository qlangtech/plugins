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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.plugin.datax.DataXMongodbReader;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-02 13:15
 **/
public class TestFlinkCDCMongoDBSourceFunction extends TestCase implements TISEasyMock {

    public void testStart() throws Exception {

        FlinkCDCMongoDBSourceFactory mongoDBSourceFactory = this.mock("mongoDBSourceFactory", FlinkCDCMongoDBSourceFactory.class);

        FlinkCDCMongoDBSourceFunction mongoDBSourceFunction = new FlinkCDCMongoDBSourceFunction(mongoDBSourceFactory);

        DataXMongodbReader mongodbReader = new DataXMongodbReader();

        List<ISelectedTab> tabs = Lists.newArrayList();
        IDataxProcessor dataXProcessor = this.mock("dataxProcess", IDataxProcessor.class);

        this.replay();

        mongoDBSourceFunction.start(mongodbReader, tabs, dataXProcessor);

        this.verifyAll();

    }
}

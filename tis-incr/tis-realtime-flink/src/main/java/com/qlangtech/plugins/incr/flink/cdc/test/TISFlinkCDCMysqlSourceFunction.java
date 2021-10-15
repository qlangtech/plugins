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

package com.qlangtech.plugins.incr.flink.cdc.test;

import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.TISFlinkSourceHandle;

//import com.qlangtech.tis.datax.impl.ESTableAlias;

//import com.qlangtech.tis.config.aliyun.IAliyunToken;
//import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
//import org.easymock.EasyMock;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 12:29
 **/
public class TISFlinkCDCMysqlSourceFunction extends IncrStreamFactory {

    private final static String tabWaitinginstanceinfo = "waitinginstanceinfo";

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
    public String name;

    @Override
    public IRCController getIncrSync() {
        FlinkTaskNodeController flinkTaskNodeController = new FlinkTaskNodeController();

        flinkTaskNodeController.setTableStreamHandle(createTableStreamHandle());

        return flinkTaskNodeController;
    }

    private BasicFlinkSourceHandle createTableStreamHandle() {
        return new TISFlinkSourceHandle();
    }

    @Override
    public String identityValue() {
        return this.name;
    }


    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IncrStreamFactory> {
        @Override
        public String getId() {
            return IncrStreamFactory.FLINK_STREM;
        }

        public DefaultDescriptor() {
            super();
        }
    }

    public static void main(String[] args) throws Exception {

        CenterResource.setNotFetchFromCenterRepository();


        // EasyMock.verify(dataXProcess, esWriter, httpToken);

    }


}

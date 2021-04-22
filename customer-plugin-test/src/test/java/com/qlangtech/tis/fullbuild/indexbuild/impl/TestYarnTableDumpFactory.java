/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.google.common.collect.Maps;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import com.qlangtech.tis.offline.TableDumpFactory;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.BaiscPluginTest;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.HeteroEnum;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author: baisui 百岁
 * @create: 2020-04-22 13:07
 **/
public class TestYarnTableDumpFactory extends BaiscPluginTest {


    public void testTrigger() {
        TableDumpFactory tabDump = HeteroEnum.DS_DUMP.getPlugin();
        assertNotNull("tabDump can not null", tabDump);

        IDumpTable table = EntityName.parse("order.totalpayinfo");

        SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");

        String startTime = f.format(new Date());

        Map<String, String> params = Maps.newHashMap();
        params.put(IParamContext.KEY_TASK_ID, "123");
        TaskContext context = TaskContext.create(params);

        IRemoteJobTrigger singleTabDumpJob = tabDump.createSingleTableDumpJob(table, context);
        assertNotNull(singleTabDumpJob);

        singleTabDumpJob.submitJob();

    }
}

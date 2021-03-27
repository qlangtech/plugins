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

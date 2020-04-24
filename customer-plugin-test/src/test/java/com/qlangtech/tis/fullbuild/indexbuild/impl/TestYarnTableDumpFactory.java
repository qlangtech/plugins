package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.dump.DumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.offline.TableDumpFactory;
import com.qlangtech.tis.util.HeteroEnum;
import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: baisui 百岁
 * @create: 2020-04-22 13:07
 **/
public class TestYarnTableDumpFactory extends TestCase {
    static {
        HttpUtils.addMockGlobalParametersConfig();
    }

    public void testTrigger() {
        TableDumpFactory tabDump = HeteroEnum.DS_DUMP.getPlugin();
        assertNotNull(tabDump);

        IDumpTable table = DumpTable.create("order", "totalpayinfo");

        SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");

        String startTime = f.format(new Date());
        TaskContext context = TaskContext.create();

        IRemoteJobTrigger singleTabDumpJob = tabDump.createSingleTableDumpJob(table, startTime, context);
        assertNotNull(singleTabDumpJob);

        singleTabDumpJob.submitJob();

    }
}

package com.qlangtech.tis.plugin.dolphinscheduler.task.impl;

import com.qlangtech.tis.plugin.dolphinscheduler.task.TISDatasyncParameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-04 15:05
 **/
public class TestDSTaskContext {
    final String workDirName = "standalone";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testGetDSServerHome() throws Exception {


        File logDir = folder.newFolder((workDirName + "/logs/1/2/4/").split("/"));
// info.log

        TISDatasyncParameters parameters = new TISDatasyncParameters();
        TaskExecutionContext taskRequest = new TaskExecutionContext();
        File logPath = (new File(logDir, "info.log"));
        FileUtils.touch(logPath);
        taskRequest.setLogPath(logPath.getAbsolutePath());
        DSTaskContext taskContext = new DSTaskContext(parameters, taskRequest);

        File workHome = taskContext.getDSServerHome();
        Assert.assertNotNull("workHome can not be null", workHome);
        Assert.assertEquals(workDirName, workHome.getName());

    }

    @Test
    public void testGetDSServerHomeNoneLogsInPath() throws Exception {
        File logDir = folder.newFolder((workDirName + "/xxxx/1/2/4/").split("/"));
// info.log

        TISDatasyncParameters parameters = new TISDatasyncParameters();
        TaskExecutionContext taskRequest = new TaskExecutionContext();
        File logPath = (new File(logDir, "info.log"));
        FileUtils.touch(logPath);
        taskRequest.setLogPath(logPath.getAbsolutePath());
        DSTaskContext taskContext = new DSTaskContext(parameters, taskRequest);

        try {
            taskContext.getDSServerHome();
            Assert.fail("shall have throw an illegalException ");
        } catch (Exception e) {
            Assert.assertTrue(StringUtils.contains( e.getMessage(),"can not find logs dir" ));
        }

    }
}

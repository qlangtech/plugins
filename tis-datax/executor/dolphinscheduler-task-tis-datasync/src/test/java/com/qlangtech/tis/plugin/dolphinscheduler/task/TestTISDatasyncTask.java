package com.qlangtech.tis.plugin.dolphinscheduler.task;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.cloud.ICoordinator;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.datax.powerjob.ExecPhase;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.dolphinscheduler.task.impl.DS4TISConfig;
import com.qlangtech.tis.plugin.dolphinscheduler.task.impl.DSTaskContext;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.junit.Assert;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-28 15:36
 **/
public class TestTISDatasyncTask extends TestCase implements TISEasyMock {

    @Override
    protected void setUp() throws Exception {
        DS4TISConfig.tisHost = "192.168.28.107";
    }

    public void testTISTableDumpProcessor() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
        TISDatasyncParameters parameters = new TISDatasyncParameters();
        parameters.varPool = JSONArray.parseArray(
                "[{\"prop\":\"app\",\"direct\":\"OUT\",\"type\":\"VARCHAR\",\"value\":\"mysql\"},{\"prop\":\"dryRun\",\"direct\":\"OUT\",\"type\":\"BOOLEAN\",\"value\":\"false\"},{\"prop\":\"execTimeStamp\",\"direct\":\"OUT\",\"type\":\"LONG\",\"value\":\"1724823048532\"},{\"prop\":\"javaMemorySpec\",\"direct\":\"OUT\",\"type\":\"VARCHAR\",\"value\":\"-Xms1351m -Xmx1801m\"},{\"prop\":\"preTaskId\",\"direct\":\"OUT\",\"type\":\"INTEGER\",\"value\":\"173\"},{\"prop\":\"taskid\",\"direct\":\"OUT\",\"type\":\"INTEGER\",\"value\":\"186\"},{\"prop\":\"pluginCfgsMetas\",\"direct\":\"OUT\",\"type\":\"VARCHAR\",\"value\":\"UEsDBBQACAgIABavHFkAAAAAAAAAAAAAAAAUAAQATUVUQS1JTkYvTUFOSUZFU1QuTUb+ygAAhZPLitswFIb3Br+DyTpWrIttJasUSqHQKaVT6KIUI9saxyBbGkkZxm/fIztpmkno7Cz5/79z0Tlx9FUMcpf43lXCmGqEUxwNk3tWuwSXhPGcsiynOYmj+KSV40tlrDYujoRzcqiVrA7aeTBsCcIFR4QjjIs4Urqr2t7uko02fgMnt4FAcRSi3XfY4+j7EKQVvZoWZSu8cPpoG1m19fg33UaPTqtwt8iUOI7NoTLaApdnPLtx+8kEsrT1dKnGqGPXjw8SVHHUKV0L9W2+evTawn9hNnM7No0e0LMSY+dlc0CARgH9ivrBKPQxfH6XAuDoNY6SQe1JRljGCccUmpjndLtu6407aHOHtCSBWjeDHud0PwngNJDEhF7POEpKzDClW5bnAactBHyXB5wz8oZXYJyXjBBarN+tFDiXYn/a3odi71RaXlDeitE9aTtAmrVwctYD52whOC8YjBf9X/RBjKKT6PMHY5Y6TlED5yowX8NIGvNFOP+g2/5p+gGz5LwYzC65eY44+ufpd8mvFYRK50JTSGHQY2rbenDpotozlCG8DytBGS4Y5xldrVfzOCfXtrcGysoyZyQnbDGkrUvnUq+VwDmLOebljfglv0/mBFb0JA7tBs6p49fCooQNIyehM6r3qRewu6kDl5fd9AYPnMVYUk4JXv0OO/MHUEsHCBP1QoX7AQAAMAQAAFBLAwQUAAgICAAWrxxZAAAAAAAAAAAAAAAACQAAAHRhc2tfeHh4eA==\"}]"
                , Property.class
        );

        JSONObject taskParams = JSONObject.parseObject("{\"localParams\":[{\"prop\":\"xxx\",\"direct\":\"IN\",\"type\":\"VARCHAR\",\"value\":\"\"}],\"resourceList\":[],\"destinationLocationArn\":\"table\",\"sourceLocationArn\":\"{\\n\\t\\\""
                + ICoordinator.KEY_DISABLE_GRPC_REMOTE_SERVER_CONNECT + "\\\":true,\\n\\t\\\"table\\\":\\\"base\\\",\\n\\t\\\"exec\\\":{\\n\\t\\t\\\"execEpochMilli\\\":0,\\n\\t\\t\\\"resType\\\":\\\"DataApp\\\",\\n\\t\\t\\\"dataXName\\\":\\\"mysql\\\",\\n\\t\\t\\\"taskSerializeNum\\\":0,\\n\\t\\t\\\"allRowsApproximately\\\":-1,\\n\\t\\t\\\"jobInfo\\\":[\\n\\t\\t\\t{\\n\\t\\t\\t\\t\\\"dataXInfo\\\":\\\"base_0.json/order/base\\\",\\n\\t\\t\\t\\t\\\"taskSerializeNum\\\":0\\n\\t\\t\\t}\\n\\t\\t],\\n\\t\\t\\\"taskId\\\":-1\\n\\t},\\n\\t\\\"dataxName\\\":\\\"mysql\\\"\\n}\",\"name\":\"mysql@base\"}");


        //  "{\\\"localParams\\\":[{\\\"prop\\\":\\\"xxx\\\",\\\"direct\\\":\\\"IN\\\",\\\"type\\\":\\\"VARCHAR\\\",\\\"value\\\":\\\"\\\"}],\\\"resourceList\\\":[],\\\"destinationLocationArn\\\":\\\"table\\\",\\\"sourceLocationArn\\\":\\\"{\\\\n\\\\t\\\\\\\"table\\\\\\\":\\\\\\\"base\\\\\\\",\\\\n\\\\t\\\\\\\"exec\\\\\\\":{\\\\n\\\\t\\\\t\\\\\\\"execEpochMilli\\\\\\\":0,\\\\n\\\\t\\\\t\\\\\\\"resType\\\\\\\":\\\\\\\"DataApp\\\\\\\",\\\\n\\\\t\\\\t\\\\\\\"dataXName\\\\\\\":\\\\\\\"mysql\\\\\\\",\\\\n\\\\t\\\\t\\\\\\\"taskSerializeNum\\\\\\\":0,\\\\n\\\\t\\\\t\\\\\\\"allRowsApproximately\\\\\\\":-1,\\\\n\\\\t\\\\t\\\\\\\"jobInfo\\\\\\\":[\\\\n\\\\t\\\\t\\\\t{\\\\n\\\\t\\\\t\\\\t\\\\t\\\\\\\"dataXInfo\\\\\\\":\\\\\\\"base_0.json/order/base\\\\\\\",\\\\n\\\\t\\\\t\\\\t\\\\t\\\\\\\"taskSerializeNum\\\\\\\":0\\\\n\\\\t\\\\t\\\\t}\\\\n\\\\t\\\\t],\\\\n\\\\t\\\\t\\\\\\\"taskId\\\\\\\":-1\\\\n\\\\t},\\\\n\\\\t\\\\\\\"dataxName\\\\\\\":\\\\\\\"mysql\\\\\\\"\\\\n}\\\",\\\"name\\\":\\\"mysql@base\\\"}";
        // System.out.println(sourceLoc);
        parameters.setDestinationLocationArn(taskParams.getString("destinationLocationArn"));
        parameters.setSourceLocationArn(taskParams.getString("sourceLocationArn"));
        TaskExecutionContext taskRequest = new TaskExecutionContext();
        File logPath = new File("/tmp/2.log");
        FileUtils.touch(logPath);
        taskRequest.setLogPath(logPath.getAbsolutePath());

        DSTaskContext taskContext = new DSTaskContext(parameters, taskRequest);

        Assert.assertTrue("isDisableGrpcRemoteServerConnect must be true", taskContext.isDisableGrpcRemoteServerConnect());

        if(taskContext.isDisableGrpcRemoteServerConnect()){
            ITISCoordinator.disableRemoteServer();
        }
        // JSONObject instanceParam = null;
        //  EasyMock.expect(taskContext.getInstanceParams()).andReturn(instanceParam);
        BasicTISTableDumpProcessor dumpProcessor = new BasicTISTableDumpProcessor();
        replay();
        String[] hasExecDataXRunCommand = new String[1];
        DataxUtils.localDataXCommandConsumer = (dataXRunCommand) -> {
            // System.out.println(dataXRunCommand);

            hasExecDataXRunCommand[0] = dataXRunCommand;
        };

        dumpProcessor.processSync(taskContext, ExecPhase.Mapper);
        String expectCommand = "java -Ddata.dir=/opt/data/tis -Denv_props=false -Dlog.dir=/opt/logs/tis -Druntime=daily -Dlogback.configurationFile=logback-datax.xml -DexecTimeStamp=1724823048532 -DlocalLoggerFilePath=/tmp/2.log -Xms1351m -Xmx1801m -DnotFetchFromCenterRepository=false -Denv_props=true -Dassemble.host=192.168.28.107 -Dtis.host=192.168.28.107 -Druntime=daily -classpath ./lib/*:./tis-datax-executor.jar:./conf/ com.qlangtech.tis.datax.DataxExecutor 186 base_0.json/order/base mysql 192.168.28.107:56432 local -1 ap 0 1724823048532 true";

        Assert.assertEquals("localDataXCommandConsumer must be execute", expectCommand, hasExecDataXRunCommand[0]);

        verifyAll();
    }
}

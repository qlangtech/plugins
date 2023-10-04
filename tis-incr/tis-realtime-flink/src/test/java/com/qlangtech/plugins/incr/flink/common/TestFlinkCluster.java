package com.qlangtech.plugins.incr.flink.common;

import com.qlangtech.tis.trigger.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/4
 */
public class TestFlinkCluster {

    @Test
    public void testInstanceSerialize() {
        FlinkCluster flinkCluster = new FlinkCluster();

        flinkCluster.jobManagerAddress = "192.168.28.201:8081";
        flinkCluster.clusterId = "my-first-flink-cluster";
        flinkCluster.name = "flink200";

        JsonUtil.assertJSONEqual(TestFlinkCluster.class, "flink-cluster-serialize.json"
                , JsonUtil.toString(flinkCluster), (message, expected, actual) -> {
            Assert.assertEquals(message, expected, actual);
        });

    }
}

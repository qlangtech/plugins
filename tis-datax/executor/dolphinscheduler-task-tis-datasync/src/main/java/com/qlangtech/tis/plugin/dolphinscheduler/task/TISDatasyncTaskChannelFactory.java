package com.qlangtech.tis.plugin.dolphinscheduler.task;

import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.plugin.SPIIdentify;

import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 **/
public class TISDatasyncTaskChannelFactory implements TaskChannelFactory {

    @Override
    public TaskChannel create() {
        return new TISDatasyncTaskChannel();
    }

    @Override
    public String getName() {
        return "DATASYNC";
    }

    @Override
    public List<PluginParams> getParams() {
        return Collections.emptyList();
    }

    public final SPIIdentify getIdentify() {
        return SPIIdentify.builder()
                .name(getName())
                .priority(9999).build();
    }
}

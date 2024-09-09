package com.qlangtech.tis.plugin.dolphinscheduler.task.impl;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-19 22:11
 **/
public class DS4TISConfig extends com.qlangtech.tis.config.BasicConfig {

    public static String tisHost;

    @Override
    protected String getAsbHost() {
        if (StringUtils.isEmpty(tisHost)) {
            throw new IllegalStateException("property tisHost can not be empty");
        }
        return tisHost;
    }

    @Override
    public String getTISHost() {
        return getAsbHost();
    }

    @Override
    public Map<String, String> getAllKV() {
        // return Collections.emptyMap();
        throw new UnsupportedOperationException();
    }
}

package com.qlangtech.tis.plugin;

import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.HttpUtils;
import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2020-05-03 11:14
 **/
public class BaiscPluginTest extends TestCase {
    static {
        HttpUtils.addMockGlobalParametersConfig();
        Config.setNotFetchFromCenterRepository();
    }
}

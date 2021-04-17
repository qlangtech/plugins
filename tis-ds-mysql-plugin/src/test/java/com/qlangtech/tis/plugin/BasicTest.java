package com.qlangtech.tis.plugin;

import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2021-04-15 16:10
 **/
public abstract class BasicTest extends TestCase {


    static {
        CenterResource.setNotFetchFromCenterRepository();
        HttpUtils.addMockGlobalParametersConfig();
    }
}

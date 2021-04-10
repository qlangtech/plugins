package com.qlangtech.tis.offline.module.action;

import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.manage.common.Option;

import java.util.Collections;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-09 12:18
 **/
public class OfflineDatasourceAction {

    public static final String DB_NAME = "baisuiMySQL";

    public static List<Option> getExistDbs(String extendClass) {
        String expectExtendClass = com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory.class.getName();
        if (!expectExtendClass.equals(extendClass)) {
            Assert.fail("param:" + extendClass + " must equal with:" + expectExtendClass);
        }

        return Collections.singletonList(new Option(DB_NAME, DB_NAME));

    }
}

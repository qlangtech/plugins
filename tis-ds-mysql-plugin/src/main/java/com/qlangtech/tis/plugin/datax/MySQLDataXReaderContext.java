package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxReaderContext;

/**
 * @author: baisui 百岁
 * @create: 2021-04-20 17:42
 **/
public class MySQLDataXReaderContext extends MySQLDataxContext implements IDataxReaderContext {
    private final String name;

    public MySQLDataXReaderContext(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

}

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxReaderContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 11:06
 **/
public class MySQLDataxContext implements IDataxContext {
    String tabName;
    String password;
    String username;
    String jdbcUrl;
    List<String> cols = new ArrayList<>();



    public String getTabName() {
        return tabName;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getColsQuotes() {
        return getColumnWithLink("\"`", "`\"");
    }

    private String getColumnWithLink(String left, String right) {
        return this.cols.stream().map(r -> (left + r + right)).collect(Collectors.joining(","));
    }
}

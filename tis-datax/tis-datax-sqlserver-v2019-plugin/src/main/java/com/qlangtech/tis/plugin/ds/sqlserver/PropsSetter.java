package com.qlangtech.tis.plugin.ds.sqlserver;

import org.apache.commons.lang3.EnumUtils;

import java.util.Properties;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/7
 * @see com.microsoft.sqlserver.jdbc.SQLServerDriverBooleanProperty
 * @see com.microsoft.sqlserver.jdbc.SQLServerDriverStringProperty
 */
public class PropsSetter {

    public static void jdbcPropsSet(Properties props, boolean useSSL) {


        setJdbcProp(props, "com.microsoft.sqlserver.jdbc.SQLServerDriverBooleanProperty", "TRUST_SERVER_CERTIFICATE", useSSL);
        setJdbcProp(props, "com.microsoft.sqlserver.jdbc.SQLServerDriverStringProperty", "ENCRYPT", useSSL);

    }

    private static void setJdbcProp(Properties props, String enumClazzName, String prop, boolean val) {
        try {
            Class<Enum> sqlserverDriverProps = (Class<Enum>) Class.forName(
                    enumClazzName, false, PropsSetter.class.getClassLoader());
            Enum e = EnumUtils.getEnum(sqlserverDriverProps, prop);
            // System.out.println(e);
            props.put(e.toString(), String.valueOf(val));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        jdbcPropsSet(null, false);
    }
}

package com.qlangtech.tis.plugin.datax.dameng.ds;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.NoneSplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class TestDaMengDataSourceFactory {


    @Test
    public void testDSConnect() {
        DaMengDataSourceFactory dsFactory = createDaMengDataSourceFactory();


        TableInDB tablesInDB = dsFactory.getTablesInDB();

        List<ColumnMetaData> cols = dsFactory.getTableMetadata(false, EntityName.parse("TEST"));
        for (ColumnMetaData col : cols) {
            System.out.println(col.getName() + ":" + col.getType());
        }
        System.out.println("tablesInDB--------------------------------------");
        tablesInDB.getTabs().forEach((tab) -> System.out.println(tab));
        System.out.println("--------------------------------------");
        dsFactory.visitAllConnection((conn) -> {
            try (Statement statement = conn.createStatement()) {

                // statement.execute("create schema tis authorization SYSDBA");

//                statement.execute("CREATE TABLE \"application_alias\"\n" +
//                        "(\n" +
//                        "    \"user_id\"     INTEGER,\n" +
//                        "    \"user_name\"   VARCHAR2(256 CHAR),\n" +
//                        "    \"id3\"         INTEGER,\n" +
//                        "    \"col4\"        VARCHAR2(256 CHAR),\n" +
//                        "    \"col5\"        VARCHAR2(256 CHAR),\n" +
//                        "    \"col6\"        VARCHAR2(256 CHAR)\n" +
//                        " , CONSTRAINT application_alias_pk PRIMARY KEY (\"user_id\")\n" +
//                        ")");


//                statement.execute("create  table tis.test(\n" +
//                        "\n" +
//                        "id int identity(1,1) primary key,\n" +
//                        "\n" +
//                        "numid  int unique,\n" +
//                        "\n" +
//                        "name varchar(20) not null,\n" +
//                        "\n" +
//                        "school  varchar(20),\n" +
//                        "\n" +
//                        "addrid int)");


//                try (ResultSet rs = statement.executeQuery("SELECT tablespace_name , (TABLE_NAME) FROM user_tables WHERE REGEXP_INSTR(TABLE_NAME,'[\\.$]+') < 1 AND tablespace_name is not null")) {
//                    while (rs.next()) {
//                        System.out.println(rs.getString(1) + "/" + rs.getString(2));
//                    }
//                }
                System.out.println("============================================");
                try (ResultSet rs = statement.executeQuery("SELECT owner ||'.'|| table_name FROM all_tables WHERE REGEXP_INSTR(table_name,'[\\.$]+') < 1")) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
                System.out.println("----------------------------------");
//                try (ResultSet rs = statement.executeQuery("SELECT TABLEDEF('TIS','TEST')")) {
//                    if (rs.next()) {
//                        System.out.println(rs.getString(1));
//                    }
//                }

                try (ResultSet rs = statement.executeQuery("SELECT TABLEDEF('TIS','application_alias')")) {
                    if (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            }
        });
    }

    public static DaMengDataSourceFactory createDaMengDataSourceFactory() {
        DaMengDataSourceFactory dsFactory = new DaMengDataSourceFactory();
        dsFactory.name = "dameng_ds";
        dsFactory.port = 30236;
        dsFactory.nodeDesc = "192.168.28.201";
        dsFactory.userName = "SYSDBA";
        dsFactory.password = "SYSDBA001";
        dsFactory.dbName = "TIS";

        dsFactory.splitTableStrategy = new NoneSplitTableStrategy();
        return dsFactory;
    }

}

package com.qlangtech.tis;

import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.offline.flattable.HiveFlatTableBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * @author: baisui 百岁
 * @create: 2020-10-13 18:06
 **/
public class Test {
    public static void main(String[] args) {
        try {
            HiveDBUtils.getInstance("192.168.28.200:10000", "xxxx").createConnection();
        } catch (Exception e) {
         //   e.printStackTrace();
//            retry:5,hivehost:jdbc:hive2://192.168.28.200:10000/xxxx
//            Cannot create PoolableConnectionFactory (Database 'xxxx' not found;)
//            Database 'xxxx' not found;
//            org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException:Database 'xxxx' not found;
            Throwable[] throwables = ExceptionUtils.getThrowables(e);
            for(Throwable t: throwables){
                System.out.println(t.getMessage());
            }
        }
    }
}

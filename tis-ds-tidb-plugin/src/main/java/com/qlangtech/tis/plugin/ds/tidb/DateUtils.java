package com.qlangtech.tis.plugin.ds.tidb;

import java.util.Calendar;
import java.util.Date;

/**
 * @author: baisui 百岁
 * @create: 2020-12-07 12:23
 **/
public class DateUtils {
    public static void main(String[] args) {
        convert(0);
    }

    public static void convert(long date) {
//        +--------+------------+------------+-----------+--------+------------+
//        | emp_no | birth_date | first_name | last_name | gender | hire_date  |
//        +--------+------------+------------+-----------+--------+------------+
//        | 499996 | 1953-03-07 | Zito       | Baaz      | M      | 1990-09-27 |
//        +--------+------------+------------+-----------+--------+------------+

//        -6144
//        7574

        Date t = new Date(0);
        Calendar instance = Calendar.getInstance();
        instance.setTime(t);
        instance.add(Calendar.DAY_OF_YEAR, -6144);
        System.out.println(instance.getTime());
    }

}

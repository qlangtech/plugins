/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.mq;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * @author: baisui 百岁
 * @create: 2020-11-02 10:11
 **/
public class TestEmployeeProducter extends BasicProducer {

    public void testProducter() throws Exception {

        DefaultMQProducer producter = this.createProducter();

//        {
//            "after": {
//               "account_num": "1.0"
//             },
//            "before": {
//               "account_num": "1.0",
//            },
//            "dbName": "order96",
//            "eventType": "UPDATE",
//            "orginTableName": "waitinginstanceinfo",
//            "targetTable": "otter_binlogorder"
//        }

        JSONObject msg = new JSONObject();
        JSONObject after = new JSONObject();
        after.put("emp_no", "9531");
        after.put("dept_no", "d999");
        after.put("from_date", "1991-04-28");
        after.put("to_date", "1991-05-09");
        msg.put("before", new JSONObject());
        msg.put("after", after);
        msg.put("dbName", "employee");
        msg.put("eventType", "INSERT");
        msg.put("orginTableName", "dept_emp");
        msg.put("targetTable", "test");


        producter.send(this.createMsg(msg.toJSONString(), "dept_emp"));

        msg = new JSONObject();
        after = new JSONObject();
        after.put("emp_no", "9531");
        after.put("dept_no", "d888");
        after.put("from_date", "1991-04-29");
        after.put("to_date", "1991-06-27");

        msg.put("before", new JSONObject());
        msg.put("after", after);
        msg.put("dbName", "employee");
        msg.put("eventType", "INSERT");
        msg.put("orginTableName", "dept_emp");
        msg.put("targetTable", "test");


        producter.send(this.createMsg(msg.toJSONString(), "dept_emp"));

    }
}

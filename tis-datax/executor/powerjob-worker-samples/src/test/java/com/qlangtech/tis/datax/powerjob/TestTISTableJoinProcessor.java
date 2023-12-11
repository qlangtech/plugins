package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.test.TISEasyMock;
import org.junit.Test;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.WorkflowContext;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/5
 */
public class TestTISTableJoinProcessor implements TISEasyMock {

    @Test
    public void testJoinProcess() throws Exception {


        TaskContext tskContext = new TaskContext();
        tskContext.setWorkflowContext(new WorkflowContext(1l, "{}"));
        tskContext.setInstanceParams("{\"app\":\"asdf\",\"dryRun\":\"false\",\"execTimeStamp\":\"1701756265952\"," +
                "\"dateParams_orderdetail\":\"1701756265952\",\"dateParams_totalpayinfo\":\"1701756265952\"," +
                "\"taskid\":\"2019\"}");

        tskContext.setJobParams("{\n" + "\t\"executeType\":\"join\",\n" + "\t\"sqlScript\":\"select t.totalpay_id,t" + ".curr_date,t.outfee,t.source_amount,t.discount_amount,t.result_amount,t.recieve_amount,t.ratio,t" + ".status,t.entity_id,t.is_valid,t.create_time,t.op_time,t.last_ver,t.op_user_id,t.discount_plan_id," + "t.operator,t.operate_date,t.card_id,t.card,t.card_entity_id,t.is_full_ratio,t.is_minconsume_ratio," + "t.is_servicefee_ratio,t.invoice_code,t.invoice_memo,t.invoice,t.over_status,t.is_hide,t.load_time," + "t.modify_time,t.printnum1,t.printnum2,t.coupon_discount,t.ext,t.discount_amount_receivables,t" + ".result_amount_receivables from totalpayinfo t inner join orderdetail o on (t.totalpay_id = o" + ".totalpay_id)\",\n" + "\t" + "\"exportName\":\"join\",\n" + "\t\"id\":\"e859b693-6d25-3322-571d" + "-4d7471a905a0\"\n" + "}");

        TISTableJoinProcessor joinProcessor = new TISTableJoinProcessor();

        joinProcessor.process(tskContext);
    }
}

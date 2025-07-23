/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax.format;

import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.format.guesstype.GuessOn;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-09 22:34
 **/
public class TestCSVFormat extends BasicFormatTest {

    @Test
    public void testCreateWriter() throws Exception {
        CSVReaderFormat csvFormat = new CSVReaderFormat();
        csvFormat.header = true;
        csvFormat.fieldDelimiter = Delimiter.Comma.token;
        csvFormat.dateFormat = "yyyy-MM-dd";
        csvFormat.nullFormat = "null";
        csvFormat.csvReaderConfig = "{\"forceQualifier\":true}";


        Assert.assertEquals(IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_true_delimiter_comma.csv")
                , testWithInsert(csvFormat));

        csvFormat.header = false;
        Assert.assertEquals(IOUtils.loadResourceFromClasspath(TestTextFormat.class, "tbl_writer_header_false_delimiter_comma.csv")
                , testWithInsert(csvFormat));

    }

    @Test
    public void testReadHeader() throws Exception {
        CSVReaderFormat csvFormat = new CSVReaderFormat();
        csvFormat.header = true;
        csvFormat.fieldDelimiter = Delimiter.Comma.token;
        csvFormat.encoding = TisUTF8.getName();
        csvFormat.dateFormat = "yyyy-MM-dd";
        csvFormat.nullFormat = "null";
        csvFormat.csvReaderConfig = "{\"forceQualifier\":false}";
        csvFormat.compress = Compress.none.token;
        GuessOn guessTypeOn = new GuessOn();
        guessTypeOn.maxInspectLine = 5000;
        csvFormat.guessFieldType = guessTypeOn;

        FileFormat.FileHeader fileHeader = IOUtils.loadResourceFromClasspath(TestTextFormat.class
                , "instancedetail_line_500.csv", true, (input) -> {
                    return csvFormat.readHeader(input);
                });

        Assert.assertNotNull(fileHeader);

        Assert.assertEquals(59, fileHeader.colCount);
        Assert.assertTrue("must contain header", fileHeader.containHeader());
        String rowCols = "instance_id,order_id,batch_msg,type,ext,waitinginstance_id,kind,parent_id,pricemode,name,makename,taste,spec_detail_name,num,account_num,unit,account_unit,price,member_price,fee,ratio,ratio_fee,ratio_cause,status,kindmenu_id,kindmenu_name,menu_id,memo,is_ratio,entity_id,is_valid,create_time,op_time,last_ver,load_time,modify_time,draw_status,bookmenu_id,make_id,make_price,prodplan_id,is_wait,specdetail_id,specdetail_price,makeprice_mode,original_price,is_buynumber_changed,ratio_operator_id,child_id,kind_bookmenu_id,specprice_mode,worker_id,is_backauth,service_fee_mode,service_fee,orign_id,addition_price,has_addition,seat_id";
        Assert.assertEquals(rowCols, fileHeader.getHeaderCols().stream().collect(Collectors.joining(",")));

        Assert.assertNotNull(" fileHeader.getTypes() can not be null", fileHeader.getTypes());


        final AtomicInteger indexAtomic = new AtomicInteger();

        String[] expectColType = StringUtils.split(
                "instance_id->varchar(64),order_id->varchar(64),batch_msg->varchar(32),type->int,ext->varchar(32),waitinginstance_id->varchar(32),kind->int,parent_id->varchar(32),pricemode->int,name->varchar(20),makename->varchar(32),taste->varchar(32),spec_detail_name->varchar(32),num->float,account_num->float,unit->varchar(2),account_unit->varchar(32),price->float,member_price->float,fee->float,ratio->float,ratio_fee->float,ratio_cause->varchar(32),status->int,kindmenu_id->varchar(64),kindmenu_name->varchar(32),menu_id->varchar(64),memo->varchar(32),is_ratio->int,entity_id->int,is_valid->int,create_time->bigint,op_time->bigint,last_ver->int,load_time->int,modify_time->int,draw_status->int,bookmenu_id->varchar(32),make_id->varchar(32),make_price->float,prodplan_id->varchar(32),is_wait->int,specdetail_id->varchar(32),specdetail_price->float,makeprice_mode->int,original_price->float,is_buynumber_changed->int,ratio_operator_id->varchar(32),child_id->varchar(32),kind_bookmenu_id->varchar(32),specprice_mode->int,worker_id->varchar(32),is_backauth->int,service_fee_mode->int,service_fee->float,orign_id->varchar(32),addition_price->float,has_addition->int,seat_id->varchar(32)", ",");
        System.out.println("type summary:");
        System.out.println(fileHeader.getHeaderCols().stream().map((colName) -> (colName + "->" + fileHeader.getTypes().get(indexAtomic.getAndIncrement()).getTypeDesc())).collect(Collectors.joining(",")));

        indexAtomic.set(0);
        for (String colName : fileHeader.getHeaderCols()) {
            Assert.assertEquals(expectColType[indexAtomic.get()], (colName + "->" + fileHeader.getTypes().get(indexAtomic.getAndIncrement()).getTypeDesc()));
        }


//        IOUtils.loadResourceFromClasspath(TestTextFormat.class
//                , "instancedetail_line_500.csv", true, (input) -> {
//                    UnstructuredReader reader = csvFormat.createReader(input);
//
//                    while (reader.hasNext()) {
//                        String[] line = reader.next();
//                        for (int i = 0; i < fileHeader.colCount; i++) {
//                            System.out.print(colsKey.get(i) + "->" + line[i]);
//                            System.out.print(",");
//                        }
//
//                        System.out.println();
//                    }
//                    return null;
//                });

    }
}

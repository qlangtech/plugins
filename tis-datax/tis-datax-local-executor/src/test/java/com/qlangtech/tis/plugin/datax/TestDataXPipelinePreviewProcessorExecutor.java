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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols.OffsetColVal;
import com.alibaba.datax.common.element.QueryCriteria;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.preview.PreviewRowsData;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-25 14:29
 **/
public class TestDataXPipelinePreviewProcessorExecutor extends TestCase {
    public void testPreviewRowsData() {
        final DataXName dataXName = DataXName.createDataXPipeline("mysql");

        DataXPipelinePreviewProcessorExecutor previewExecutor = new DataXPipelinePreviewProcessorExecutor(51509);
        PreviewProgressorExpireTracker expireTracker = new PreviewProgressorExpireTracker(dataXName.getPipelineName(), 999999);
        previewExecutor.setCommitTracker(expireTracker);
        String identityVal = null;
        boolean next = true;
        boolean needHeader = true;
        int pageSize = 1;
        QueryCriteria queryCriteria = new QueryCriteria();
        queryCriteria.setPagerOffsetCursor(null);
        queryCriteria.setNextPakge(next);
        queryCriteria.setPageSize(pageSize);
        PreviewRowsData previewRowsData = previewExecutor.previewRowsData(dataXName, "base", queryCriteria);
        Assert.assertNotNull(previewRowsData);
    }

    public void testPreviewRowsDataForDameng() {

        final String dataXName = "dameng_mysql";

        DataXPipelinePreviewProcessorExecutor previewExecutor = new DataXPipelinePreviewProcessorExecutor(51509);
        PreviewProgressorExpireTracker expireTracker = new PreviewProgressorExpireTracker(dataXName, 999999);
        previewExecutor.setCommitTracker(expireTracker);
        //String identityVal = null;
        boolean next = true;
        //  boolean needHeader = true;
        int pageSize = 20;
        // QueryCriteria queryCriteria = new QueryCriteria();

        JSONObject jsonPostContent = JSONObject.parseObject("{\"nextPage\":true,\"offsetPointer\":[{\"val\":\"000012184fb5165f014fb51722460038\",\"numeric\":false,\"key\":\"pay_id\"}]}");
        QueryCriteria queryCriteria = QueryCriteria.createCriteria(pageSize, jsonPostContent);

//        List<OffsetColVal> pagerOffsetCursor = OffsetColVal.deserializePreviewCursor(jsonPostContent.get);
//        queryCriteria.setPagerOffsetCursor(pagerOffsetCursor);
//        queryCriteria.setNextPakge(next);
//        queryCriteria.setPageSize(pageSize);
        PreviewRowsData previewRowsData = previewExecutor.previewRowsData(
                DataXName.createDataXPipeline( dataXName), "TIS.payinfo", queryCriteria);
        Assert.assertNotNull(previewRowsData);


    }
}

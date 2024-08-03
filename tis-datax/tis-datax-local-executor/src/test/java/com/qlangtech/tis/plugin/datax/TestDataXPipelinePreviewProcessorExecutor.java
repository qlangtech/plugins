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

import com.alibaba.datax.common.element.QueryCriteria;
import com.qlangtech.tis.datax.preview.PreviewRowsData;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-25 14:29
 **/
public class TestDataXPipelinePreviewProcessorExecutor extends TestCase {
    public void testPreviewRowsData() {
        final String dataXName = "mysql";
        DataXPipelinePreviewProcessorExecutor previewExecutor = new DataXPipelinePreviewProcessorExecutor(51509);
        PreviewProgressorExpireTracker expireTracker = new PreviewProgressorExpireTracker(dataXName, 999999);
        previewExecutor.setCommitTracker(expireTracker);
        String identityVal = null;
        boolean next = true;
        boolean needHeader = true;
        int pageSize = 10;
        QueryCriteria queryCriteria = new QueryCriteria();
        queryCriteria.setPagerOffsetPointCols(null);
        queryCriteria.setNextPakge(next);
        queryCriteria.setPageSize(pageSize);
        PreviewRowsData previewRowsData = previewExecutor.previewRowsData(dataXName, "orderdetail", queryCriteria);
        Assert.assertNotNull(previewRowsData);
    }
}

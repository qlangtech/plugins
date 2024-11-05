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

package com.qlangtech.tis.realtime;

import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-10 15:07
 **/
public class SelectedTableTransformerRules {
    private final RecordTransformerRules transformerRules;
    private final ISelectedTab tab;
    private final IFlinkColCreator<FlinkCol> sourceFlinkColCreator;
    private final IPluginContext dataXContext;
    private final ITransformerBuildInfo rules;

    public SelectedTableTransformerRules(RecordTransformerRules transformerRules, ISelectedTab tab
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator, IPluginContext dataXContext) {
        this.transformerRules = Objects.requireNonNull(transformerRules, "param transformerRules can not be null");
        this.tab = Objects.requireNonNull(tab, "param tab can not be null");
        this.sourceFlinkColCreator = Objects.requireNonNull(sourceFlinkColCreator, "param sourceFlinkColCreator can not be null");
        this.dataXContext = Objects.requireNonNull(dataXContext, "param dataXContext can not be null");
        this.rules = transformerRules.createTransformerBuildInfo(dataXContext);
    }

    List<OutputParameter> cols;

    public List<OutputParameter> overwriteColsWithContextParams() {
        if (cols == null) {
            cols = rules.overwriteColsWithContextParams(this.tab.getCols());
        }
        return cols;
    }

    public List<FlinkCol> transformerColsWithoutContextParamsFlinkCol() {
        this.overwriteColsWithContextParams();
        return FlinkCol.getAllTabColsMeta(rules.tranformerColsWithoutContextParams(), sourceFlinkColCreator);
    }

    public List<FlinkCol> originColsWithContextParamsFlinkCol() {
//        ITransformerBuildInfo transformerBuildInfo = rules;
//        transformerBuildInfo.overwriteColsWithContextParams(tab.getCols());
        this.overwriteColsWithContextParams();
        return FlinkCol.getAllTabColsMeta(this.rules.originColsWithContextParams(), sourceFlinkColCreator);
    }

    public RecordTransformerRules getTransformerRules() {
        return this.transformerRules;
    }

    public ISelectedTab getTab() {
        return this.tab;
    }

    public IFlinkColCreator<FlinkCol> getSourceFlinkColCreator() {
        return this.sourceFlinkColCreator;
    }

    public IPluginContext getDataXContext() {
        return this.dataXContext;
    }
}

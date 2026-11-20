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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * 该模块可离线运行的单元测试聚合套件(由测试验证脚本自动整理生成)。
 * 依赖数据库/本地文件/内网服务/Testcontainers 的用例见 TEST_SKIP_REPORT.md。
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    com.qlangtech.tis.plugin.ontology.EnableChatBITest.class,
    com.qlangtech.tis.plugin.ontology.TestStreamingJsonOntologyParser.class,
    com.qlangtech.tis.plugin.ontology.TestStreamingJsonOntologyParserDebug.class,
    com.qlangtech.tis.plugin.ontology.aggregation.TestAggregationCriteria.class,
    com.qlangtech.tis.plugin.ontology.chatbi.FalconChatBITestExampleTest.class,
    com.qlangtech.tis.plugin.ontology.chatbi.TestTraceStep.class,
    com.qlangtech.tis.plugin.ontology.chatbi.validation.TestAstValidator.class,
    com.qlangtech.tis.plugin.ontology.chatbi.validation.TestExplainValidator.class,
    com.qlangtech.tis.plugin.ontology.chatbi.validation.TestKeywordWhitelistValidator.class,
    com.qlangtech.tis.plugin.ontology.chatbi.validation.TestPrestoParserDebug.class,
    com.qlangtech.tis.plugin.ontology.chatbi.validation.TestPrestoParserDebug2.class,
    com.qlangtech.tis.plugin.ontology.graphrag.DefaultGraphRAGServiceITTest.class,
    com.qlangtech.tis.plugin.ontology.graphrag.DefaultGraphRAGServiceTest.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionFormDescriptorBuilder.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionParameterMapper.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionRelevantPayload.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionRuleRidCascade.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionRuleSetterPersistence.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionRulesMulti.class,
    com.qlangtech.tis.plugin.ontology.impl.action.TestActionStepChain.class,
    com.qlangtech.tis.plugin.ontology.impl.action.type.TestLinkRuleSupport.class,
    com.qlangtech.tis.plugin.ontology.impl.action.type.TestMultiObjectTypeNamespace.class,
    com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1Test.class,
    com.qlangtech.tis.plugin.ontology.impl.objtype.TestDataSourceBindingAggregation.class,
    com.qlangtech.tis.plugin.ontology.impl.objtype.TestObjectSetRowQuery.class,
    com.qlangtech.tis.plugin.ontology.impl.objtype.aggregation.TestAggregationSqlBuilder.class,
    com.qlangtech.tis.plugin.ontology.workshop.TestWidgetDisplayReconcile.class,
    com.qlangtech.tis.plugin.ontology.workshop.TestWorkshopFormDefaults.class,
    com.qlangtech.tis.plugin.ontology.workshop.TestWorkshopPageFormRoundTrip.class,
    com.qlangtech.tis.plugin.ontology.workshop.WorkshopModuleReloadTest.class,
    com.qlangtech.tis.plugin.ontology.workshop.desc.TestNoLegacyVariableOperationEndpoint.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.TestConditionalVisibilitySplit.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.TestSecondaryMetricSplit.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.TestTimeSeriesVisualizationSplit.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.TestWorkshopOverlayFormRoundTrip.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.TestWorkshopVariableTypes.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.config.TestVariableBasedVisibilitySplit.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.config.TestWorkshopConfigPlugins.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.definition.TestObjectSetFilterCriteria.class,
    com.qlangtech.tis.plugin.ontology.workshop.model.section.TestWorkshopSectionConfig.class,
    com.qlangtech.tis.plugin.ontology.workshop.service.TestVariableReferenceScanner.class,
    com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopLayoutStoreTest.class,
    com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStoreTest.class,
    com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopWidgetDisplayStoreTest.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.TestFilterItemConfigContract.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.TestWidgetColumnConfigColumnMapping.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.component.TestHistogramComponentPayload.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.component.TestHistogramDataProcessor.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.component.TestOpsSourceOptions.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ObjectListWidgetTest.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestNoLegacyObjectSetEndpoint.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestObjectListActiveObjectSelection.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestObjectListWidgetMaxObjects.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestObjectListWidgetSortingAndSelection.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestObjectTableWidgetPagenator.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TestObjectTableWidgetSortOrder.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.TestAggregationPushdownPlanner.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.TestChartXYAggregation.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.TestTemporalBucketFormat.class,
    com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.TestTemporalBucketStart.class,
})
public class TestAll {
}

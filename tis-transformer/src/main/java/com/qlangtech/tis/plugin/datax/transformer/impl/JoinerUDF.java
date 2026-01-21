package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.InParamer;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSelectDataSource;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSelectTable;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSetMatchConditionAndCols;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchConditionCreatorFactory;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerUDF extends UDFDefinition implements MultiStepsSupportHost {

    private OneStepOfMultiSteps[] stepsPlugin;

    @Override
    public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
        return this.stepsPlugin;
    }

    @Override
    public List<OutputParameter> outParameters() {
        return Lists.newArrayList();
    }

    @Override
    public List<InParamer> inParameters() {
        return Lists.newArrayList();
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {

    }

    @Override
    public List<UDFDesc> getLiteria() {
        JoinerSetMatchConditionAndCols conditionAndCols = this.getOneStepOf(OneStepOfMultiSteps.Step.Step3);
        JoinerSelectTable selectTable = this.getOneStepOf(OneStepOfMultiSteps.Step.Step2);
        JoinerSelectDataSource selectDS = this.getOneStepOf(OneStepOfMultiSteps.Step.Step1);
        List<UDFDesc> literia = Lists.newArrayList();
        List<UDFDesc> selectDesc = Lists.newArrayList();
        List<CMeta> cols = conditionAndCols.targetCols;
        int totalCols = cols.size();
        String colsDisplay = cols.stream()
                .limit(5)
                .map((col) -> "'" + col.getName() + "'")
                .collect(Collectors.joining(","));
        if (totalCols > 5) {
            colsDisplay += "...共" + totalCols + "列";
        }
        selectDesc.add(new UDFDesc("Cols", colsDisplay));

        selectDesc.add(new UDFDesc("DataSource", selectDS.dbName));
        selectDesc.add(new UDFDesc("Prefix", conditionAndCols.colPrefix));

        literia.add(new UDFDesc("Select", selectDesc));
        AtomicInteger conditionNum = new AtomicInteger(0);
        literia.add(new UDFDesc("With Match"
                , conditionAndCols.matchCondition.stream()
                .map((mc) -> new UDFDesc("Condition" + conditionNum.incrementAndGet()
                        , mc.getPrimaryTableMatchColName() + "=" + selectTable.tagetTable + "." + mc.getDimensionMatchColName())).collect(Collectors.toList())));
        literia.add(new UDFDesc("Cache", conditionAndCols.cache.isOn() ? "enable" : "disabled"));
        return literia;
    }

    @Override
    public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
        this.stepsPlugin = stepsPlugin;
    }


    @TISExtension
    public static class DefaultDescriptor extends UDFDefinition.BasicUDFDesc implements MultiStepsSupportHostDescriptor<JoinerUDF> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.Constant;
        }


        @Override
        public String getDisplayName() {
            return "Joiner Outer Table";
        }

        @Override
        public Class<JoinerUDF> getHostClass() {
            return JoinerUDF.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return Lists.newArrayList(new JoinerSelectDataSource.Desc()
                    , new JoinerSelectTable.Desc()
                    , new JoinerSetMatchConditionAndCols.Desc());
        }

        @Override
        public void appendExternalProps(JSONObject multiStepsCfg) {
            List<CMeta> sourceTabCols = SelectedTab.getSelectedCols();
            if (CollectionUtils.isEmpty(sourceTabCols)) {
                throw new IllegalStateException("sourceTabCols can not be empty");
            }
            multiStepsCfg.put(TableJoinMatchConditionCreatorFactory.KEY_SOURCE_TAB_COLS, sourceTabCols);
        }
    }

}

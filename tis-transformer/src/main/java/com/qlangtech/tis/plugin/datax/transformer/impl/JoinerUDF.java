package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
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

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerUDF extends UDFDefinition {


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
        return Lists.newArrayList();
    }


    @TISExtension
    public static class DefaultDescriptor extends UDFDefinition.BasicUDFDesc implements MultiStepsSupportHost {
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

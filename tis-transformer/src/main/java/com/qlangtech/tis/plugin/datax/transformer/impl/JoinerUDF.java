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
 * Joiner UDF for joining primary table with dimension table
 * <p>
 * This UDF implements a multi-step configuration process:
 * <ol>
 *   <li>Step 1: Select data source ({@link JoinerSelectDataSource})</li>
 *   <li>Step 2: Select target table ({@link JoinerSelectTable})</li>
 *   <li>Step 3: Set match conditions and columns ({@link JoinerSetMatchConditionAndCols})</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 * @see MultiStepsSupportHost
 * @see JoinerSelectDataSource
 * @see JoinerSelectTable
 * @see JoinerSetMatchConditionAndCols
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

    /**
     * Maximum number of columns to display in the description
     */
    private static final int MAX_DISPLAY_COLS = 5;

    /**
     * Get the literia (description) of this UDF for display purposes
     * <p>
     * This method builds a human-readable description of the join configuration,
     * including selected columns, data source, match conditions, and cache settings.
     *
     * @return List of UDFDesc describing the join configuration
     */
    @Override
    public List<UDFDesc> getLiteria() {
        // Get all step configurations
        JoinerSetMatchConditionAndCols conditionAndCols = this.getOneStepOf(OneStepOfMultiSteps.Step.Step3);
        JoinerSelectTable selectTable = this.getOneStepOf(OneStepOfMultiSteps.Step.Step2);
        JoinerSelectDataSource selectDS = this.getOneStepOf(OneStepOfMultiSteps.Step.Step1);

        List<UDFDesc> literia = Lists.newArrayList();

        // Add select information
        literia.add(buildSelectDesc(conditionAndCols, selectDS));

        // Add match conditions
        literia.add(buildMatchConditionDesc(conditionAndCols, selectTable));

        // Add cache status
        literia.add(new UDFDesc("Cache", conditionAndCols.cache.isOn() ? "enable" : "disabled"));

        return literia;
    }

    /**
     * Build the select description including columns, data source, and prefix
     *
     * @param conditionAndCols Configuration from step 3
     * @param selectDS         Data source from step 1
     * @return UDFDesc for select information
     */
    private UDFDesc buildSelectDesc(JoinerSetMatchConditionAndCols conditionAndCols,
                                    JoinerSelectDataSource selectDS) {
        List<UDFDesc> selectDesc = Lists.newArrayList();

        // Format columns display
        String colsDisplay = formatColumnsDisplay(conditionAndCols.targetCols);
        selectDesc.add(new UDFDesc("Cols", colsDisplay));

        selectDesc.add(new UDFDesc("DataSource", selectDS.dbName));
        selectDesc.add(new UDFDesc("Prefix", conditionAndCols.colPrefix));

        return new UDFDesc("Select", selectDesc);
    }

    /**
     * Format columns for display
     * Shows up to MAX_DISPLAY_COLS columns, with ellipsis if there are more
     *
     * @param cols List of columns
     * @return Formatted string like "'col1','col2','col3'...total 10 columns"
     */
    private String formatColumnsDisplay(List<CMeta> cols) {
        int totalCols = cols.size();
        String colsDisplay = cols.stream()
                .limit(MAX_DISPLAY_COLS)
                .map(col -> "'" + col.getName() + "'")
                .collect(Collectors.joining(","));

        if (totalCols > MAX_DISPLAY_COLS) {
            colsDisplay += String.format("...total %d columns", totalCols);
        }

        return colsDisplay;
    }

    /**
     * Build match condition descriptions
     *
     * @param conditionAndCols Configuration from step 3
     * @param selectTable      Table selection from step 2
     * @return UDFDesc for match conditions
     */
    private UDFDesc buildMatchConditionDesc(JoinerSetMatchConditionAndCols conditionAndCols,
                                            JoinerSelectTable selectTable) {
        AtomicInteger conditionNum = new AtomicInteger(0);
        List<UDFDesc> matchConditions = conditionAndCols.matchCondition.stream()
                .map(mc -> new UDFDesc(
                        "Condition" + conditionNum.incrementAndGet(),
                        String.format("%s=%s.%s",
                                mc.getPrimaryTableMatchColName(),
                                selectTable.tagetTable,
                                mc.getDimensionMatchColName())))
                .collect(Collectors.toList());

        return new UDFDesc("With Match", matchConditions);
    }

    /**
     * Set all step plugins to this host
     * <p>
     * This method is called by the framework to inject all step configurations
     * after user completes the multi-step wizard.
     *
     * @param stepsPlugin Array of step plugins in order
     */
    @Override
    public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
        this.stepsPlugin = stepsPlugin;
    }


    /**
     * Descriptor for JoinerUDF
     * <p>
     * Defines the multi-step configuration process for joining tables.
     */
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

        /**
         * Get the list of step descriptions for the multi-step wizard
         * <p>
         * Defines three steps:
         * <ol>
         *   <li>Select data source</li>
         *   <li>Select target table</li>
         *   <li>Set match conditions and columns</li>
         * </ol>
         *
         * @return List of step descriptions
         */
        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return Lists.newArrayList(new JoinerSelectDataSource.Desc()
                    , new JoinerSelectTable.Desc()
                    , new JoinerSetMatchConditionAndCols.Desc());
        }

        /**
         * Append external properties to the multi-step configuration
         * <p>
         * Adds source table columns to the context so they can be used
         * in match condition UI.
         *
         * @param multiStepsCfg Configuration object to append properties to
         * @throws IllegalStateException if source table columns are not available
         */
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

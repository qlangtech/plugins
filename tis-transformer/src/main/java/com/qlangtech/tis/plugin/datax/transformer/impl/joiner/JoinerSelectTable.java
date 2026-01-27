package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchConditionCreatorFactory;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerSelectTable extends OneStepOfMultiSteps {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String tagetTable;

    /**
     * 目标记录是否开启缓存，这样会加速join速度，如果缓存中存在就直接从缓存中获取
     */
    @FormField(ordinal = 4, validate = {Validator.require})
    public TargetRowsCache cache;

    public static List<Option> selectableTabs() {
        JoinerSelectDataSource prevPlugin = getPreviousStepInstance(JoinerSelectDataSource.class);
        List<String> existTabs = prevPlugin.getDataSourceFactory().getTablesInDB().getTabs();
        return existTabs.stream().map(Option::new).collect(Collectors.toList());
    }

    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context currentCtx, OneStepOfMultiSteps[] preSavedStepPlugins) {
        currentCtx.put(TableJoinMatchConditionCreatorFactory.getTargetTableColsKey() //
                , reflectTabCols(preSavedStepPlugins));
    }

    public List<ColumnMetaData> reflectTabCols(OneStepOfMultiSteps[] preSavedStepPlugins) {
        try {
            JoinerSelectDataSource selectDataSource = (JoinerSelectDataSource) preSavedStepPlugins[Step.Step1.getStepIndex()];
            return selectDataSource.reflectTabCols(tagetTable);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {
        public Desc() {
            super();
        }

        @Override
        public Step getStep() {
            return Step.Step2;
        }

        @Override
        public String getDisplayName() {
            return "第二步";
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc() {
            return Optional.of(new JoinerSetMatchConditionAndCols.Desc());
        }

        @Override
        public String getStepDescription() {
            return "选择需要Join的目标表";
        }
    }
}

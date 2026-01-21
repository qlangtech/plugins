package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchConditionCreatorFactory;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerSetMatchConditionAndCols extends OneStepOfMultiSteps {

    /**
     * 定义join match 规则，例如：source.order_id = target.order_id，可以定义多个关联条件
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<TableJoinMatchCondition> matchCondition;
    /**
     * 输出列会加上这个前缀，这样可以保证和主表列有区别，避免列冲突
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String colPrefix;

    @FormField(ordinal = 2, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<CMeta> targetCols;

    /**
     * 目标记录是否开启缓存，这样会加速join速度，如果缓存中存在就直接从缓存中获取
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public TargetRowsCache cache;

    /**
     * 取得目标端列集合
     *
     * @return
     */
    public static List<CMeta> getTargetCols() {
        return TableJoinMatchConditionCreatorFactory.getTargetCols();
    }

    public static List<TableJoinMatchCondition> getCondition() {
        return Collections.emptyList();
    }

    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context currentCtx, OneStepOfMultiSteps[] preSavedStepPlugins) {

    }

    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc implements FormFieldType.IMultiSelectValidator {
        public Desc() {
            super();
        }

        @Override
        public Step getStep() {
            return Step.Step3;
        }

        @Override
        public String getDisplayName() {
            return "第三步";
        }

        @Override
        public String getStepDescription() {
            return "选择匹配条件和Join表输出列";
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc() {
            return Optional.empty();
        }
    }
}

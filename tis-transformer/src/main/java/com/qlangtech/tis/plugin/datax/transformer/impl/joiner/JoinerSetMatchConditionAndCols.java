package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchConditionCreatorFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerSetMatchConditionAndCols extends OneStepOfMultiSteps implements Serializable {
    private static final String FIELD_TARGET_COLS = "targetCols";
    /**
     * 定义join match 规则，例如：source.order_id = target.order_id，可以定义多个关联条件
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<TableJoinMatchCondition> matchCondition;

    /**
     * 过滤条件（可选）：在JOIN时对主表或维表进行过滤
     * 例如：A.valid='1' AND B.valid='1'
     */
    @FormField(ordinal = 1, advance = false, type = FormFieldType.MULTI_SELECTABLE)
    public List<TableJoinFilterCondition> filterConditions;

    /**
     * 输出列会加上这个前缀，这样可以保证和主表列有区别，避免列冲突
     */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String colPrefix;

    @FormField(ordinal = 3, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<CMeta> targetCols;

    @FormField(ordinal = 4, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean skipError;


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

    public static List<TableJoinFilterCondition> getFilterConditions() {
        return Collections.emptyList();
    }

    @Override
    protected void processPreSaved(IPluginContext pluginContext, Context currentCtx, OneStepOfMultiSteps[] preSavedStepPlugins) {
        IControlMsgHandler msgHandler = (IControlMsgHandler) pluginContext;
        JSONObject postContent = null;
        if ((postContent = pluginContext.getJSONPostContent()) != null) {
            JSONArray sourceCols = postContent.getJSONArray(TableJoinMatchConditionCreatorFactory.KEY_SOURCE_TAB_COLS);
            List<String> pcols = sourceCols.stream().map((c) -> (String) ((JSONObject) c).get(CMeta.FIELD_NAME)).toList();
            //JoinerSelectDataSource step1 = (JoinerSelectDataSource) preSavedStepPlugins[0];
            //JoinerSelectTable step2 = (JoinerSelectTable) preSavedStepPlugins[Step.Step2.getStepIndex()];
            // List<ColumnMetaData> pcols = step2.reflectTabCols(preSavedStepPlugins);

            final Set<String> targetColSet = this.targetCols.stream().map(CMeta::getName)
                    .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            Set<String> duplicateCols
                    = pcols.stream()
                    .filter(targetColSet::contains)
                    .collect(Collectors.toSet());

            if (CollectionUtils.isNotEmpty(duplicateCols)) {
                msgHandler.addFieldError(currentCtx, FIELD_TARGET_COLS, "被选列："
                        + duplicateCols.stream().map((col) -> "'" + col + "'")
                        .collect(Collectors.joining(",")) + "与源表列存在重复");
            }
        }


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
        public Optional<BasicDesc> nextPluginDesc(OneStepOfMultiSteps current) {
            return Optional.empty();
        }


        @Override
        public String getStepDescription() {
            return "选择匹配条件和Join表输出列";
        }

//        @Override
//        public Optional<BasicDesc> nextPluginDesc() {
//            return Optional.empty();
//        }
    }
}

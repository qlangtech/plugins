package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectSetRowQuery;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ButtonGroupType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ExportClickAction;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetButtonConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Map;

/**
 * P0 Widget：按钮组（Button Group）
 * <p>
 * 按钮组容器，支持水平/垂直布局、多种按钮类型、条件可见性，
 * 以及事件 / 本体动作 / 链接 / 导出四种点击行为。
 * <p>
 * 每个按钮的完整形状（文案、形态、图标、条件可见性、点击行为）由
 * {@link WidgetButtonConfig} 承担；点击行为本身是 {@code WidgetClickAction} 多态，
 * 对应前端的四种分支。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ButtonGroupWidget extends WorkshopWidget {

    public static final String KEY_BUTTONS = "buttons";

    /** `get_data` 的 query-string 参数：要导出的对象集变量名 */
    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";

    /** `get_data` 的 query-string 参数：期望行数上限（缺省 {@link ObjectSetRowQuery#DEFAULT_MAX_ROWS}） */
    public static final String KEY_LIMIT = "limit";

    /** 按钮组的呈现形态：内联平铺 / 折叠为下拉菜单 / 主按钮 + 菜单两段式（Palantir 的「Button type」）。
     *  改为 {@link ButtonGroupType} 多态承载 —— 仅内联平铺子类带 {@code layout} 载荷，另两个子类零字段。
     *  {@link Validator#require} 与 {@code dftVal=inline} 同 {@code ObjectTableWidget.pagenator} 口径：始终有值，防旧数据缺字段落 null。 */
    @FormField(ordinal = 9,validate = {Validator.require})
    public ButtonGroupType buttonType;

    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = WidgetButtonConfig.class,ordinal = 10,validate = {Validator.require})
    public List<WidgetButtonConfig> buttons;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        /**
         * 导出数据用的服务端取数。
         *
         * <p>参数走 <b>query string</b> 而不是 POST 表单投影：导出的对象集变量配在
         * {@code ExportClickAction} 上，要投影它得先投影整棵按钮树（{@code buttons[]} 是多态子表单，
         * 里面再嵌一层 {@code onClick} 多态）—— 为了两个标量参数做这件事既脆又慢。
         * 点击时现取也正是想要的语义：导出的是「此刻」的数据，不是打开页面那一刻的快照。
         *
         * @see ExportClickAction
         */
        @Override
        public void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
            String methodType = paramGetter.getString("type");
            switch (methodType) {
                case "get_data": {
                    String objectSetVar = paramGetter.getString(KEY_OBJECT_SET_VAR);
                    if (StringUtils.isBlank(objectSetVar)) {
                        throw new IllegalStateException("param '" + KEY_OBJECT_SET_VAR + "' can not be empty");
                    }
                    int maxRows = NumberUtils.toInt(paramGetter.getString(KEY_LIMIT), ObjectSetRowQuery.DEFAULT_MAX_ROWS);

                    WorkshopVariable variable = WorkshopVariable.loadDetail(objectSetVar);
                    if (!(variable instanceof ObjectSetVariable objSetVar)) {
                        throw new IllegalStateException("变量 '" + objectSetVar + "' 不是对象集合变量，实际类型:"
                                + variable.getClass().getSimpleName());
                    }
                    // 导出即全属性：用户导出的是配置里看到的那张表，不是某个 widget 关心的几列
                    List<String> targetCols = ObjectSetRowQuery.allColNames(objSetVar.getColsMeta());
                    List<RowMap> rows = objSetVar.fetchRows(targetCols, maxRows);
                    paramGetter.setBizResult(context, Map.of("rows", rows));
                    return;
                }
                default:
                    throw new IllegalStateException("illegal methodType:" + methodType);
            }
        }

        @Override
        public String getDisplayName() {
            return "Button Group";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.BUTTON_GROUP;
        }
    }
}

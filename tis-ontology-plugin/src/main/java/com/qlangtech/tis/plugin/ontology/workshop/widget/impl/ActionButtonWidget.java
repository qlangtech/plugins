package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * Action Button：Ontology Action 触发按钮
 * <p>
 * 触发 Ontology Action 执行，支持加载状态、条件禁用、成功/失败提示。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ActionButtonWidget extends WorkshopWidget {

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Action Button";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.ACTION_BUTTON;
        }
    }
}
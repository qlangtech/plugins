package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.HashMap;
import java.util.Map;

/**
 * Workshop Widget Descriptor 统一基类
 * <p>
 * 职责：在 <b>final</b> {@link #getExtractProps()} 中统一组装前端的提取属性
 * （icon、category、widgetType），各子类只需实现 {@link #getWidgetType()} 返回
 * 对应的枚举常量即可。
 * <p>
 * 如果某个 Widget 需要携带额外提取属性（如 {@code defaultSize}），可覆写
 * {@link #appendExtractProps(Map)} 方法。
 *
 * @param <T> Widget 类型参数，通常为 {@link IWorkshopWidget} 的子类型
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public abstract class BaseWidgetDescriptor<T extends IWorkshopWidget> extends Descriptor<T> {

    /**
     * 子类返回对应的 Widget 类型枚举
     */
    public abstract WorkShopWidgetType getWidgetType();

    /**
     * final 方法 —— 从 {@link #getWidgetType()} 中统一读取 icon / category / widgetType，
     * 并合并 {@link #appendExtractProps(Map )} 中的附加属性。
     * 子类<b>不得</b>覆写此方法；需要额外属性请覆写 {@link #appendExtractProps(Map)}。
     */
    @Override
    public final Map<String, Object> getExtractProps() {
        WorkShopWidgetType type = getWidgetType();
        Map<String, Object> props = new HashMap<>();
        props.put("icon", type.icon);
        props.put("category", type.category.val);
        props.put("widgetType", type.widgetType);
        // 合并子类的额外属性（如 defaultSize）
        return appendExtractProps(props);

    }

    /**
     * 可选的额外提取属性钩子 —— 默认返回空 Map。
     * 子类可覆写此方法来添加 {@code defaultSize} 等非通用属性。
     */
    protected Map<String, Object> appendExtractProps(Map<String, Object> props) {
        return props;
    }
}
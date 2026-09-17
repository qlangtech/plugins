package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 「开启」形态的变量路由配置：变量值变化后跳到本 Workshop 的某个页面，并把值带进 URL 参数。
 *
 * <p>开关由子类类型承担，故本类没有任何 {@code enabled} 字段。对照 {@link NoneRoutingConfig}。
 *
 * <p>注意与 {@code workshop.model.RoutingConfig} 是两个不同的概念，后者是模块级的
 * 路由基址等设置；本类只描述「变量变化 → 跳页」这一个动作，故不叫 RoutingConfig。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class PageRoutingConfig extends VariableRoutingConfig {

    public static final String KEY_TARGET_PAGE = "targetPage";
    public static final String KEY_PARAMS = "params";

    /**
     * 目标页面名（当前 domain 下的页面，候选项见
     * {@link WidgetOptionHelper#getWorkshopPageOptions()}）。
     *
     * <p>存的是页面 <b>name</b> 而非 id：与变量一致，模块内引用一律用名字，
     * 页面 id 是自动生成的 UUID，只决定落盘文件名。跳转发生时按名字查页面。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String targetPage;

    /**
     * 路由参数映射表，每行是「URL 参数名 → 模块内变量名」。
     *
     * <p>非必填：空列表表示只跳页、不带参，是合法形态
     * （{@link RoutingParamCreatorFactory} 对 null 行数组同样按空列表处理）。
     */
    @FormField(ordinal = 1, type = FormFieldType.MULTI_SELECTABLE)
    public List<RoutingParamRow> params = new ArrayList<>();

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            // SELECTABLE 必须在 Descriptor 里注册选项，否则渲染时抛
            // "fieldName:targetPage is select options has not been register"
            this.registerSelectOptions(KEY_TARGET_PAGE, WidgetOptionHelper::getWorkshopPageOptions);
        }

        @Override
        public String getDisplayName() {
            return "Page Routing";
        }

        @Override
        public String shortComment() {
            return "跳转到指定页面";
        }
    }
}

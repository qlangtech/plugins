package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.alibaba.citrus.turbine.Context;

import java.util.Objects;
import java.util.Optional;

/**
 * Workshop 布局实体（page / overlay / section / widget / header / variable）统一的
 * 「表单解析 → 校验 → 实例化」入口。
 *
 * <h3>为什么必须走这条链路</h3>
 * 前端提交的是插件表单的 POST 形态（{@code {impl, vals}}，叶子包 {@code _primaryVal}、
 * 嵌套描述符包 {@code descVal}）。框架里的映射关系是：
 * <ul>
 *   <li>{@link AttrValMap#parseDescribableMap}：按 {@code impl} 找到 descriptor 并还原字段值；</li>
 *   <li>{@link AttrValMap#validateWithScope}：按 descriptor 做必填 / {@code Validator} /
 *       嵌套子表单校验，<b>并把 descriptor 写进返回结果</b>；</li>
 *   <li>{@link Descriptor.PluginValidateResult#newInstance()}：用校验通过的那份表单值实例化。</li>
 * </ul>
 *
 * <p>三步缺一不可。以前这里用 {@code attrValMap.createDescribable(...).getInstance()}：
 * 它只解析不校验，字段缺失 / 枚举值非法 / 子表单不完整都会一路带进落盘流程。
 * 也不能用裸的 {@code validate()} 代替 {@code validateWithScope()}：前者不写 descriptor，
 * {@code newInstance()} 会直接抛 {@code descriptor can not be null}
 * （参见 {@code DefaultOntologyAction#executePreviewForm}，本类与其同一套写法）。
 *
 * <p>校验失败时，字段级错误已由 {@code msgHandler} 写进 {@link Context}；这里再抛异常是为了
 * <b>中止后续写盘</b> —— 尤其 {@code module-save} 一次要写四段实体，任何一段不合法都不应留下
 * 半写状态。
 */
public final class WorkshopFormParser {

    private WorkshopFormParser() {
    }

    /**
     * @param itemJson     POST 形态的表单项：{@code {"impl":..., "vals":{...}}}
     * @param label        错误信息里的实体定位（如 {@code page[0]}、{@code widget[xxx]}）
     * @param expectedType 期望的实体类型。widget 是多实现，因此用 {@code isInstance} 而不是
     *                     比较 impl —— 后者只对「一类一 descriptor」的实体成立
     */
    public static <T> T parse(IControlMsgHandler msgHandler, Context context,
                              JSONObject itemJson, String label, Class<T> expectedType) {
        Objects.requireNonNull(itemJson, label + " item can not be null");
        String impl = itemJson.getString(AttrValMap.PLUGIN_EXTENSION_IMPL);
        AttrValMap attrValMap = AttrValMap.parseDescribableMap(Optional.empty(), itemJson);

        Descriptor.PluginValidateResult validateResult =
                attrValMap.validateWithScope(msgHandler, context, 0, 0, FormVaildateType.STRICT);
        if (!validateResult.isValid()) {
            throw new IllegalStateException(label + " 表单校验未通过（impl=" + impl + "）");
        }

        Object instance = validateResult.newInstance();
        if (!expectedType.isInstance(instance)) {
            throw new IllegalStateException("expect entity type " + expectedType.getName()
                    + " but is " + instance.getClass().getName() + " (impl=" + impl + ")");
        }
        return expectedType.cast(instance);
    }
}

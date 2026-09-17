package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 「开启」形态的变量状态保存配置：把变量值写进外部存储，跨会话保留。
 *
 * <p>改造前本类对应的是一个 {@code boolean enabled} + {@code storageKey} 的
 * POJO，允许出现「{@code enabled == false} 却带着 {@code storageKey}」这种
 * 自相矛盾的数据。现在开关由<b>子类类型</b>承担，{@link NoneStateSavingConfig}
 * 上根本不存在 {@code storageKey} 字段，矛盾态在类型层面即不可表示。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/16
 */
public class PersistentStateSavingConfig extends VariableStateSavingConfig {

    public static final String KEY_STORAGE_KEY = "storageKey";

    /**
     * 外部存储中的键名。开启保存后本字段才有意义，故它只需出现在本子类上。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String storageKey;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Persistent";
        }

        @Override
        public String shortComment() {
            return "持久化变量值";
        }
    }
}

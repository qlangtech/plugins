package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.manage.common.Option;

/**
 * 带计数（以及「空值行」标记）的候选项。
 *
 * <p>线上形状是 {@code {label, val, count}}；空值行额外带 {@code nullValue: true}。
 * 两个键都由 {@link #appendExtraProps(JSONObject)} 追加 —— 那是 {@code Option.toJson}
 * 留给子类的唯一扩展点，其余 20 余处候选项序列化逐字节不变。
 *
 * <h3>为什么计数挂在 Option 上而不是另开一个并列数组</h3>
 * 候选项与它的计数必须<b>同一个下标</b>对齐。并列数组（{@code options: [...]} +
 * {@code counts: [...]}）在任何一侧被过滤 / 排序之后就静默错位，而错位的计数看起来
 * 完全正常 —— 只是数字不对。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/5
 * @see ObjectSetOpsSource
 */
public class OptionWithCount extends Option {

    public static final String KEY_COUNT = "count";
    public static final String KEY_NULL_VALUE = "nullValue";

    /**
     * 空值行的哨兵值。真实属性值不可能等于它 —— 它带 {@code __} 前缀，且只在
     * 「该属性确实存在 NULL」时才会出现；前端据 {@link #KEY_NULL_VALUE} 把它翻译成
     * {@code isNull} / {@code inOrNull} 谓词，自己从不写进输出变量。
     */
    public static final String NULL_OPTION_VALUE = "__tis_null__";

    private final long count;

    private final boolean nullValue;

    private OptionWithCount(String name, String value, long count, boolean nullValue) {
        super(name, value);
        this.count = count;
        this.nullValue = nullValue;
    }

    public static OptionWithCount of(String label, String value, long count) {
        return new OptionWithCount(label, value, count, false);
    }

    /**
     * 空值行：显示成 {@code No value}（前端按 {@link #KEY_NULL_VALUE} 加斜体），
     * 值固定为 {@link #NULL_OPTION_VALUE}，永远不会被当成真实属性值写进谓词。
     */
    public static OptionWithCount nullValue(String label, long count) {
        return new OptionWithCount(label, NULL_OPTION_VALUE, count, true);
    }

    public long getCount() {
        return count;
    }

    public boolean isNullValue() {
        return nullValue;
    }

    @Override
    public void appendExtraProps(JSONObject o) {
        o.put(KEY_COUNT, count);
        if (nullValue) {
            o.put(KEY_NULL_VALUE, true);
        }
    }
}

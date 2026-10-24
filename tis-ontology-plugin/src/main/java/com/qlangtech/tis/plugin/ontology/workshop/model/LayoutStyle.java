package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;

import java.io.Serializable;

/**
 * 指标卡的布局样式（抽象基类，多态）。
 *
 * <p>三种布局各自带不同的可配参数，因此用子类而不是「一个枚举 + 几个可为空的参数字段」：
 * <ul>
 *   <li>{@link CardLayout} —— 卡片，可配 {@link Direction} 与 {@link Template}</li>
 *   <li>{@link TagLayout} —— 标签，只可配 {@link Direction}</li>
 *   <li>{@link ListLayout} —— 列表，只可配 {@link Template}</li>
 * </ul>
 * 扁平化后「选了 Tag 却还填着 template」这种自相矛盾的状态能编译、能过校验；
 * 多态把载荷交给子类，编译器就能约束住。
 *
 * <p><b>本类刻意不声明任何 {@code @FormField} 字段</b>（字段全在子类，ordinal 从 0 起），
 * 也<b>不得</b>带 {@code @TISExtension} 或嵌套描述符 —— sezpoz 索引不过滤抽象类，
 * 加了会让抽象类型静默混进插件选择面板。
 *
 * <p><b>子类命名必须以 {@code Layout} 结尾。</b> 前端
 * {@code resolveImplKind(impl, 'Layout')} 以「剥后缀 + PascalCase 转 kebab-case」推导短 key，
 * 故 {@code CardLayout} → {@code card}、{@code TagLayout} → {@code tag}、
 * {@code ListLayout} → {@code list}。改名会让前端的分支静默失配（整块渲染成空白，不报错）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @see com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionLayout
 */
public abstract class LayoutStyle implements Describable<LayoutStyle>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<LayoutStyle> {
    }

    /**
     * 多个指标之间的排列方向。
     * <p>
     * 实现 {@link DescriptorUseableShortComment} 后，{@code shortComment()} 会被
     * {@code OptionWithEndType.createEnumOptions} 反射成选项的 help 下发前端，
     * json 资源里<b>不再</b>声明 {@code enum} 数组。
     */
    public enum Direction implements DescriptorUseableShortComment {
        HORIZONTAL("水平"),
        VERTICAL("垂直");

        public final String label;

        Direction(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    /**
     * 单个指标内「标签」与「数值」的排布方式。
     */
    public enum Template implements DescriptorUseableShortComment {
        STACKED("上下堆叠"),
        SIDE_BY_SIDE("左右并排");

        public final String label;

        Template(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }
}

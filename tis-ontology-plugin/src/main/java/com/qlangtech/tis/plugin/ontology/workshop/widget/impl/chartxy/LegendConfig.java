package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 图例的「显示」实现：位置是本子表单专有的字段。
 *
 * <p>图例内容（系列名与配色）由各图层的 series 自动生成，此处只配放在哪。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public class LegendConfig extends AbstractLegendConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 图例位置；默认下方。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public LegendPosition legendPosition = LegendPosition.BOTTOM;

    /**
     * 图例位置。
     */
    public enum LegendPosition implements DescriptorUseableShortComment {
        TOP("上方"),
        BOTTOM("下方"),
        LEFT("左侧"),
        RIGHT("右侧");

        private final String comment;

        LegendPosition(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Legend";
        }

        @Override
        public String shortComment() {
            return "显示图例";
        }
    }
}

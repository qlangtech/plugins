package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;

import java.util.List;

/**
 * 子分区：{@link WorkshopSection#childSections} 的表单行元素。
 * <p>
 * 与 {@link WorkshopSection} 共享 {@link AbstractWorkshopSection} 的全部字段，但<b>刻意不含
 * {@code childSections}</b> —— 把 section 嵌套从「任意深度自引用」收敛为「有界一层」，以断掉
 * {@code WorkshopSection → childSections → WorkshopSection → …} 的自引用，规避
 * {@code DescriptorsJSON.getDescriptorsJSON()} 无环检测/无深度上限导致的 StackOverflowError。
 * <p>
 * 本类作为 {@code MULTI_DESCRIBLE_PLUGIN} 的元素类，必须 {@code implements MultiDescribleElement}
 * （= {@code ManipuldateProcessor + IdentityName}）；identity 已在基类 {@link AbstractWorkshopSection}
 * 的 UUID {@code id} 上声明，此处无需重复。name 只做展示 / 引用键，故不用 {@code Validator.identity}。
 */
public class SectionChild extends AbstractWorkshopSection
        implements Describable<SectionChild>, IPluginStore.MultiDescribleElement {

    @TISExtension
    public static class DefaultDescriptor extends IPluginStore.MultiDescribleElementDescriptor<SectionChild> {

        @Override
        public String shortComment() {
            return "子分区";
        }

        @Override
        public String getDisplayName() {
            return "Section Child";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少有一列被标记为 clickable：点击该列可打开该条记录的编辑对话框
            return Lists.newArrayList(new ColConfig("name", 15).setClickable(), new ColConfig("layout"));
        }
        // 缺省即可：isEnumableSet()=false（开放集合，自由添加）、viewStyle()=List、maxHeight()=100
        // 若以后要表格视图，需保证 colsConfig() 至少一列 .setClickable()（getExtractProps 强校验）
    }
}

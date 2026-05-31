package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import org.apache.commons.lang.StringUtils;

/**
 * 主键设置
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/31
 */
public abstract class OntologyPrimaryKeySetter implements Describable<OntologyPrimaryKeySetter> {

    /**
     * 设置主键
     *
     * @param propertieSetStep
     */
    public abstract void setPrimaryKey(ObjectTypeProperties propertieSetStep);


    public abstract boolean hasDisablePK();

    /**
     * 可能为关系表没有主键
     */
    public static class NonePrimaryKey extends OntologyPrimaryKeySetter {
        @Override
        public boolean hasDisablePK() {
            return true;
        }

        @Override
        public void setPrimaryKey(ObjectTypeProperties propertieSetStep) {

        }

        @TISExtension
        public static class DefaultDesc extends Descriptor<OntologyPrimaryKeySetter> implements DescriptorUseableShortComment {
            @Override
            public String getDisplayName() {
                return SWITCH_OFF;
            }

            @Override
            public String shortComment() {
                return "没有主键";
            }
        }
    }

    public static class PrimaryKey4OneField extends OntologyPrimaryKeySetter {
        @FormField(type = FormFieldType.ENUM, ordinal = 0, validate = {Validator.require,
                Validator.db_col_name})
        public String pkField;

        @Override
        public boolean hasDisablePK() {
            return false;
        }

        @Override
        public void setPrimaryKey(ObjectTypeProperties propertieSetStep) {
            if (StringUtils.isEmpty(this.pkField)) {
                throw new IllegalStateException("pkField can not be empty");
            }
            for (OntologyProperty prop : propertieSetStep.cols) {
                if (StringUtils.equals(prop.name, pkField)) {
                    prop.setPk(true);
                }
            }
        }

        @TISExtension
        public static class DefaultDesc extends Descriptor<OntologyPrimaryKeySetter> implements DescriptorUseableShortComment {
            @Override
            public String getDisplayName() {
                return SWITCH_ON;
            }

            @Override
            public String shortComment() {
                return "选择一个属性作为主键";
            }
        }
    }
}

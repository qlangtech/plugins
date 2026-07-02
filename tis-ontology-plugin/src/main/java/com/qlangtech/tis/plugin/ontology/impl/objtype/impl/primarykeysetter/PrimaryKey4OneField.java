package com.qlangtech.tis.plugin.ontology.impl.objtype.impl.primarykeysetter;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeProperties;
import com.qlangtech.tis.plugin.ontology.impl.objtype.OntologyPrimaryKeySetter;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/7/2
 */
public class PrimaryKey4OneField extends OntologyPrimaryKeySetter {
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

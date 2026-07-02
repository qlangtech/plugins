package com.qlangtech.tis.plugin.ontology.impl.objtype.impl.primarykeysetter;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeProperties;
import com.qlangtech.tis.plugin.ontology.impl.objtype.OntologyPrimaryKeySetter;

/**
 * 可能为关系表没有主键
 */
public class NonePrimaryKey extends OntologyPrimaryKeySetter {
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

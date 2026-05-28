package com.qlangtech.tis.plugin.ontology.impl.linker;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class LinkerUtils {
    public static LinkResources.BasicLinkResourceDesc getObjectTypeLinkerDescriptor(RelationshipType relationshipType) {
        switch (relationshipType) {
            case JoinTableDataset -> {
                return new RelationshipTypeJoinTableDataset.DefDesc();
            }
            case BackingObjectType -> {
                return new RelationshipTypeBackingObjectType.DefDesc();
            }
            case ObjectTypeForeignKeys -> {
                return new RelationshipTypeObjectTypeForeignKeys.DefDesc();
            }
        }
        throw new IllegalStateException("illegal relationshipType:" + relationshipType);
    }

    private LinkerUtils() {

    }
}

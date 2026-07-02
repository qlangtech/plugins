package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.extension.Describable;

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

}

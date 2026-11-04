package com.qlangtech.tis.plugin.ontology.workshop;

import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptType;
import com.qlangtech.tis.util.AttrValMap;

import java.util.Objects;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/1
 */
public class WorkshopFormParserException extends Exception {
    private final AttrValMap attrValMap;

//    private static final Set<Class<?>> acceptExpectedTypes;
//
//    static {
//        acceptExpectedTypes = Sets.newHashSet();
//        // 这里需要初始化设置
//    }

    private final WorkshopCptType cptType;

    public WorkshopFormParserException(AttrValMap attrValMap, WorkshopCptType cptType, String message) {
        super(message);
        this.attrValMap = Objects.requireNonNull(attrValMap, "attrValMap can not be null");
        this.cptType = Objects.requireNonNull(cptType, "cptType can not be null");
//        if (!acceptExpectedTypes.contains(expectedType)) {
//            throw new IllegalStateException("the param expectedType:" + expectedType + " is not accepted");
//        }
    }

    public String getExpectedType() {
        return cptType.name();
    }

    public String getPrimaryIDVal() {
        return attrValMap.getPrimaryFieldVal();
    }
}

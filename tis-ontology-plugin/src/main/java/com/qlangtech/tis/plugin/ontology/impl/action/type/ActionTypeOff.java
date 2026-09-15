package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

import java.util.List;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class ActionTypeOff extends ActionType {
    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.OFF;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of();
    }

    @Override
    public OntologyActionRule getRule() {
       throw new UnsupportedOperationException();
    }

    @TISExtension
    public static class DefaultDescriptor extends ActionType.BaseActionTypeDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.OFF;
        }

        @Override
        public String getDisplayName() {
            return getRuleGroup().getDisplayName();
        }
    }
}

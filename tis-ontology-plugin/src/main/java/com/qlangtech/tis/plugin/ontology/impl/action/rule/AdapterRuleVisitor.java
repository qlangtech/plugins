package com.qlangtech.tis.plugin.ontology.impl.action.rule;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/18
 */
public class AdapterRuleVisitor implements RuleVisitor {
    private final boolean throwExceptionWhenUnResolve;

    public AdapterRuleVisitor(boolean throwExceptionWhenUnResolve) {
        this.throwExceptionWhenUnResolve = throwExceptionWhenUnResolve;
    }

    @Override
    public void visit(CreateObjectRule rule) {
        check();
    }

    private void check() {
        if (throwExceptionWhenUnResolve) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void visit(ModifyObjectRule rule) {
        check();
    }

    @Override
    public void visit(DeleteObjectRule rule) {
        check();
    }

    @Override
    public void visit(CreateLinkRule rule) {
        check();
    }

    @Override
    public void visit(DeleteLinkRule rule) {
        check();
    }

    @Override
    public void visit(FunctionBackedRule rule) {
        check();
    }

    @Override
    public void visit(NotificationRule rule) {
        check();
    }

    @Override
    public void visit(WebhookRule rule) {
        check();
    }
}

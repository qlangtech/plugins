package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * 页面模板枚举
 */
public enum PageTemplate implements DescriptorUseableShortComment {
    BLANK("空白页"),
    INBOX("收件箱布局"),
    DASHBOARD("仪表板布局"),
    DETAIL("详情页布局"),
    TWO_COLUMN("两栏布局"),
    THREE_COLUMN("三栏布局");

    private final String comment;

    PageTemplate(String comment) {
        this.comment = comment;
    }

    @Override
    public String shortComment() {
        return comment;
    }
}

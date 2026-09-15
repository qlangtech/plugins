package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * Workshop Section 布局类型枚举
 */
public enum SectionLayout implements DescriptorUseableShortComment {
  COLUMNS("列布局"),
  ROWS("行布局"),
  TABS("标签页布局"),
  FLOW("流式布局"),
  TOOLBAR("工具栏布局"),
  LOOP("循环布局");

  private final String comment;

  SectionLayout(String comment) {
    this.comment = comment;
  }

  @Override
  public String shortComment() {
    return comment;
  }
}
package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * Workshop Overlay 类型枚举
 */
public enum OverlayType implements DescriptorUseableShortComment {
  DRAWER("抽屉"),
  MODAL("模态框");

  private final String comment;

  OverlayType(String comment) {
    this.comment = comment;
  }

  @Override
  public String shortComment() {
    return comment;
  }
}
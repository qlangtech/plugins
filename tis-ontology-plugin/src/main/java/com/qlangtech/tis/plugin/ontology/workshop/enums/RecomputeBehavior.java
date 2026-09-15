package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * Workshop Variable 重新计算行为枚举
 */
public enum RecomputeBehavior implements DescriptorUseableShortComment {
  AUTOMATIC("自动计算"),
  ON_TRIGGER("触发时计算"),
  ON_LOAD_AND_TRIGGER("加载和触发时计算");

  private final String comment;

  RecomputeBehavior(String comment) {
    this.comment = comment;
  }

  @Override
  public String shortComment() {
    return comment;
  }
}
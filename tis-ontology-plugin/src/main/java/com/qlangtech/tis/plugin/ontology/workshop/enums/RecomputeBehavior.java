package com.qlangtech.tis.plugin.ontology.workshop.enums;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;

/**
 * Workshop Variable 重新计算行为枚举
 *
 * <p>⚠️ 自 2026-11 起变量在前端**按需计算**（只有被 widget 订阅时才计算），
 * 因此 {@link #ON_LOAD_AND_TRIGGER} 里的「加载时」这一半已没有执行点 ——
 * 模块加载不再计算任何变量。三个常量都保留只为落盘兼容（存量 XML 里存的是常量名）。
 *
 * <p>{@link #AUTOMATIC} 与另外两个的差别仍然生效：只有 AUTOMATIC 的变量会随上游变更被
 * 传播重算（且下游必须**有订阅者**才真的重算，见前端 {@code VariableService#propagateChange}）。
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
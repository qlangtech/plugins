package com.qlangtech.tis.plugin.ontology.workshop.model.overlay;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Workshop Overlay 类型配置抽象基类
 * 定义 Overlay 的具体显示行为和样式
 */
public abstract class OverlayTypeConfig implements Describable<OverlayTypeConfig> {

  protected abstract static class BasicDescriptor extends Descriptor<OverlayTypeConfig> {
    // 公共描述符逻辑
  }
}
package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import java.io.Serializable;

/**
 * Workshop Variable 状态保存配置
 * 定义变量值的持久化策略
 */
public class VariableStateSavingConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 是否启用状态保存 */
  private boolean enabled = false;

  /** 存储键名 */
  private String storageKey;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getStorageKey() {
    return storageKey;
  }

  public void setStorageKey(String storageKey) {
    this.storageKey = storageKey;
  }
}
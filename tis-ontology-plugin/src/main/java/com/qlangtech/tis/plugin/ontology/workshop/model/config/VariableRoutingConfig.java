package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Workshop Variable 路由配置
 * 定义变量值变化后的页面跳转行为
 */
public class VariableRoutingConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 目标页面名称 */
  private String targetPage;

  /** 路由参数 */
  private Map<String, String> params = new HashMap<>();

  public String getTargetPage() {
    return targetPage;
  }

  public void setTargetPage(String targetPage) {
    this.targetPage = targetPage;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }
}
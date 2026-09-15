package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Workshop Variable 接口配置
 * 定义变量与外部接口的交互方式
 */
public class VariableInterfaceConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 接口 ID */
  private String interfaceId;

  /** 输入参数映射 */
  private List<InterfaceInput> inputs = new ArrayList<>();

  public String getInterfaceId() {
    return interfaceId;
  }

  public void setInterfaceId(String interfaceId) {
    this.interfaceId = interfaceId;
  }

  public List<InterfaceInput> getInputs() {
    return inputs;
  }

  public void setInputs(List<InterfaceInput> inputs) {
    this.inputs = inputs;
  }

  /**
   * 接口输入参数
   */
  public static class InterfaceInput implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 参数名 */
    private String parameter;

    /** 引用的变量 ID */
    private String variableId;

    public String getParameter() {
      return parameter;
    }

    public void setParameter(String parameter) {
      this.parameter = parameter;
    }

    public String getVariableId() {
      return variableId;
    }

    public void setVariableId(String variableId) {
      this.variableId = variableId;
    }
  }
}
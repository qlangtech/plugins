package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * Workshop Variable 不存在异常
 */
public class VariableNotFoundException extends RuntimeException {

  public VariableNotFoundException(String message) {
    super(message);
  }

  public VariableNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
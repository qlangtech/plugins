package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * Workshop Variable 已存在异常
 */
public class VariableAlreadyExistsException extends RuntimeException {

  public VariableAlreadyExistsException(String message) {
    super(message);
  }

  public VariableAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
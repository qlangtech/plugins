package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * 变量循环依赖异常
 */
public class CircularDependencyException extends RuntimeException {

  public CircularDependencyException(String message) {
    super(message);
  }

  public CircularDependencyException(String message, Throwable cause) {
    super(message, cause);
  }
}
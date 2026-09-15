package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * Workshop Module 不存在异常
 */
public class ModuleNotFoundException extends RuntimeException {

  public ModuleNotFoundException(String message) {
    super(message);
  }

  public ModuleNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
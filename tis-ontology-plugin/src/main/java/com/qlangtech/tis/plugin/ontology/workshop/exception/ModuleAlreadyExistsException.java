package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * Workshop Module 已存在异常
 */
public class ModuleAlreadyExistsException extends RuntimeException {

  public ModuleAlreadyExistsException(String message) {
    super(message);
  }

  public ModuleAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }
}
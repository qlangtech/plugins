package com.qlangtech.tis.plugin.ontology.workshop.exception;

/**
 * Workshop Module 保存异常
 */
public class ModuleSaveException extends RuntimeException {

  public ModuleSaveException(String message) {
    super(message);
  }

  public ModuleSaveException(String message, Throwable cause) {
    super(message, cause);
  }
}
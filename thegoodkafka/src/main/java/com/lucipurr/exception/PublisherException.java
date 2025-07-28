/* (C) Lucipurr 69@420 */
package com.lucipurr.exception;

import java.io.Serial;

public class PublisherException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public PublisherException(String s) {
    super(s);
  }
}

// XmlKeyExtractor.java
package com.hcltech.rmg.xml;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.io.InputStream;
import java.util.Optional;

/** Extracts a key from XML without building a DOM. Implementations must be thread-safe. */
public interface XmlKeyExtractor {
  /** Extract the key from a UTF-8 XML string. */
  ErrorsOr<String> extractId(String xml, String idPath);

}

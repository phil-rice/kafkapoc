package com.hcltech.rmg.celcore;

import java.util.List;

/** References discovered in the expression (for tooling/validation). */
public interface RuleUsage {
  List<String> inputPaths();
  List<String> contextPaths();
}

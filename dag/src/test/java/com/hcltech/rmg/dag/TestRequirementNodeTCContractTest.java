package com.hcltech.rmg.dag;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concrete NodeTC contract tests for TestRequirementFixture.TestRequirementNode (String paths).
 * Verifies owns/produces, requires, and label via the real NodeTC from the fixture.
 */
public final class TestRequirementNodeTCContractTest extends AbstractNodeTCContractTest<TestRequirementFixture.TestRequirementNode, String> {

  @Override
  protected NodeTC<TestRequirementFixture.TestRequirementNode, String> ntc() {
    return TestRequirementFixture.ntc; // the inline NodeTC from the fixture
  }

  @Override
  protected String p(String path) {
    return path; // string paths: identity
  }

  @Override
  protected TestRequirementFixture.TestRequirementNode makeNode(String label, String[] producesDot, String[] requiresDot) {
    return new TestRequirementFixture.TestRequirementNode(
        label,
        toLinkedHashSet(producesDot),
        toLinkedHashSet(requiresDot)
    );
  }

  private static Set<String> toLinkedHashSet(String[] arr) {
    return new LinkedHashSet<>(Arrays.asList(arr));
  }

  // (Optional) a couple of explicit sanity tests in addition to the contract:

  @Test
  void label_passthrough() {
    var n = makeNode("MyNode", new String[]{"a.b"}, new String[]{"a.b.c"});
    assertEquals("MyNode", ntc().label(n));
  }

  @Test
  void emptySets_areHandled() {
    var n = makeNode("Empty", new String[]{}, new String[]{});
    assertTrue(ntc().owns(n).isEmpty());
    assertTrue(ntc().requires(n).isEmpty());
  }
}

package com.hcltech.rmg.dag;

import java.util.*;

import static java.util.Arrays.asList;

/**
 * Reusable fixture for requirement-graph tests.
 * - Uses String paths (readable)
 * - Provides TestRequirementNode
 * - Exposes PathTC<String> and an inline NodeTC<TestRequirementNode,String>
 * - Includes handy helpers and common node constants
 */
public final class TestRequirementFixture {

    // ---------- Test node ----------
    public static record TestRequirementNode(String label, Set<String> produces, Set<String> requires) {}

    // ---------- Typeclasses ----------
    public static PathTC<String> ptc() { return new StringPathTC(); }

    public static NodeTC<TestRequirementNode, String> ntc = new NodeTC<>() {
        @Override public Set<String> owns(TestRequirementNode n)     { return n.produces(); }
        @Override public Set<String> requires(TestRequirementNode n) { return n.requires(); }
        @Override public String label(TestRequirementNode n)         { return n.label(); }
    };

    // ---------- Builders / helpers ----------
    public static TestRequirementNode node(String label, String[] produces, String[] requires) {
        return new TestRequirementNode(label, setP(produces), setP(requires));
    }

    public static Set<String> setP(String... paths) {
        return new LinkedHashSet<>(asList(paths));
    }

    @SafeVarargs
    public static <T> Set<T> set(T... xs) {
        return new LinkedHashSet<>(asList(xs));
    }

    /** Build a RequirementGraph from nodes using the tested typeclasses. */
    public static RequirementGraph<TestRequirementNode> build(Set<TestRequirementNode> nodes) {
        return RequirementGraphBuilder.build(nodes, ptc(), ntc);
    }

    /** Topologically sort a RequirementGraph into generations (Kahn). */
    public static List<Set<TestRequirementNode>> sort(RequirementGraph<TestRequirementNode> g) {
        return Topo.topoSort(g);
    }

    // ---------- Common nodes for tests ----------

    // Producers on 'a' branch
    public static final TestRequirementNode A            = node("A",            new String[]{"a"},     new String[]{});
    public static final TestRequirementNode AB           = node("AB",           new String[]{"a.b"},   new String[]{});
    public static final TestRequirementNode AB_DUP       = node("AB_DUP",       new String[]{"a.b"},   new String[]{}); // duplicate exact root
    public static final TestRequirementNode ABC_OWNER    = node("ABC_OWNER",    new String[]{"a.b.c"}, new String[]{}); // prefix-overlap with AB

    // Consumers on 'a' branch
    public static final TestRequirementNode ABC          = node("ABC",          new String[]{},        new String[]{"a.b.c"});
    public static final TestRequirementNode ABX          = node("ABX",          new String[]{},        new String[]{"a.b.x"});
    public static final TestRequirementNode ACY          = node("ACY",          new String[]{},        new String[]{"a.c.y"});

    // Separate branches (for join/fork)
    public static final TestRequirementNode B_OWNER      = node("B_OWNER",      new String[]{"b"},     new String[]{});
    public static final TestRequirementNode C_OWNER      = node("C_OWNER",      new String[]{"c"},     new String[]{});
    public static final TestRequirementNode D_JOIN       = node("D_JOIN",       new String[]{},        new String[]{"b.x", "c.y"});

    // External requirement (no producer present)
    public static final TestRequirementNode EXT          = node("EXT",          new String[]{},        new String[]{"z.y"});

    // Disconnected producers
    public static final TestRequirementNode X1           = node("X1",           new String[]{"x"},     new String[]{});
    public static final TestRequirementNode Y1           = node("Y1",           new String[]{"y"},     new String[]{});

    // Cycle trio (A needs under C; B under A; C under B)
    public static final TestRequirementNode A_NEEDS_C    = node("A_NEEDS_C",    new String[]{"a"},     new String[]{"c.x"});
    public static final TestRequirementNode B_NEEDS_A    = node("B_NEEDS_A",    new String[]{"b"},     new String[]{"a.y"});
    public static final TestRequirementNode C_NEEDS_B    = node("C_NEEDS_B",    new String[]{"c"},     new String[]{"b.z"});

    private TestRequirementFixture() {}
}

package com.example.optics;

import org.apache.commons.jxpath.JXPathContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for IOpticsEvent with JXPath-backed SetEvent/AppendEvent. */
final class OpticsEventTest {

    // --- simple bean model used in tests (mutable for JXPath) ---

    public static class Person {
        private String name;
        private List<String> tags = new ArrayList<>();
        private Address address = new Address();

        public Person() { }
        public Person(String name, List<String> tags) {
            this.name = name;
            this.tags = new ArrayList<>(tags);
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public List<String> getTags() { return tags; }
        public void setTags(List<String> tags) { this.tags = tags; }

        public Address getAddress() { return address; }
        public void setAddress(Address address) { this.address = address; }
    }

    public static class Address {
        private String street;

        public String getStreet() { return street; }
        public void setStreet(String street) { this.street = street; }
    }

    // -------------------- Tests --------------------

    @Test
    void setEvent_updates_field_and_preserves_others() {
        Person p = new Person("Alice", List.of("a", "b"));
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> ev = IOpticsEvent.setEvent("name", "Bob");

        JXPathContext out = ev.apply(ctx);

        assertSame(ctx, out, "apply should return the same context");
        assertEquals("Bob", p.getName());
        assertEquals(List.of("a","b"), p.getTags());
    }

    @Test
    void setEvent_nested_path_updates_nested_property() {
        Person p = new Person("Alice", List.of());
        // ensure nested bean exists (JXPath.setValue does not auto-create here)
        p.setAddress(new Address());
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> ev = IOpticsEvent.setEvent("address/street", "Main St");

        ev.apply(ctx);

        assertEquals("Main St", p.getAddress().getStreet());
        assertEquals("Alice", p.getName());
    }

    @Test
    void appendEvent_appends_to_list_in_place() {
        Person p = new Person("Alice", List.of("x", "y"));
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> ev = IOpticsEvent.appendEvent("tags", "z");

        ev.apply(ctx);

        assertEquals(List.of("x","y","z"), p.getTags());
        assertEquals("Alice", p.getName());
    }

    @Test
    void appendEvent_handles_empty_list() {
        Person p = new Person("Alice", List.of());
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> ev = IOpticsEvent.appendEvent("tags", "one");

        ev.apply(ctx);

        assertEquals(List.of("one"), p.getTags());
    }

    @Test
    void composition_set_then_append_applies_in_order() {
        Person p = new Person("Alice", List.of("t1"));
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> setName = IOpticsEvent.setEvent("name", "Carol");
        IOpticsEvent<JXPathContext> addTag  = IOpticsEvent.appendEvent("tags", "t2");

        setName.apply(ctx);
        addTag.apply(ctx);

        assertEquals("Carol", p.getName());
        assertEquals(List.of("t1","t2"), p.getTags());
    }

    @Test
    void appendEvent_throws_when_path_is_not_a_list() {
        Person p = new Person("Alice", List.of("x"));
        JXPathContext ctx = JXPathContext.newContext(p);

        IOpticsEvent<JXPathContext> ev = IOpticsEvent.appendEvent("name", "should-fail");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ev.apply(ctx));
        assertTrue(ex.getMessage().contains("Path does not point to a list"));
        // state unchanged
        assertEquals("Alice", p.getName());
        assertEquals(List.of("x"), p.getTags());
    }
}

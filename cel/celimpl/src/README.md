# CEL Builder (Java)

This module provides a **fluent builder** around [Google’s Common Expression Language (CEL)](https://github.com/google/cel-spec) via the [cel-java](https://github.com/google/cel-java) library.  
It lets us:

- Compile CEL expressions into executable programs
- Optionally validate AST, input paths, and context paths
- Execute CEL safely, returning results via an `ErrorsOr<T>` monad instead of throwing
- Inspect which `input.*` and `context.*` fields are referenced by a rule

---

## Why

We wanted a clean, hexagonal-friendly interface for CEL that:

- Keeps CEL details out of the domain code
- Lets us validate rules against known schemas (e.g. XML → map)
- Fails fast at compile/validation time instead of runtime
- Is execution-time efficient (no runtime data conversions)

---
## ⚠️ Warnings

When providing `input` and `context` maps to CEL:

- **Use `Long` for integers**  
  CEL’s `int` is 64-bit. Supplying a Java `Integer` (e.g., `2`) will cause operator overload errors.  
  Always use `2L`, `150L`, etc.


- **Prefer bracket access**  
  Use `input["field"]` and `context["field"]` instead of dotted access (`input.field`).  
  Bracket syntax works reliably with maps and avoids ambiguity.

- **Allowed value types**  
  CEL supports `Boolean`, `Long`, `Double`, `String`, nested `Map<String, ?>`, and `List<?>`.  
  Arbitrary POJOs are not supported unless converted to a CEL-compatible form.

- **Null handling**  
  `null` values are allowed but may cause evaluation errors if used with operators.  
  Guard with presence checks, e.g.:
  ```cel
  "amount" in input ? input["amount"] > 0 : false


## Basic Example

```java
import com.hcltech.rmg.celimpl.CelBuilder;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.Map;

public class Demo {
    public static void main(String[] args) {
        // Simple rule: check user and threshold
        String rule = """
                context["user"] == "admin" &&
                input["amount"] > 100
                """;

        var compiledOr = CelBuilder.celBuilder(rule).compile();

        if (compiledOr.isError()) {
            System.out.println("Compile errors: " + compiledOr.getErrors());
            return;
        }

        var compiled = compiledOr.valueOrThrow();

        Map<String, Object> input = Map.of("amount", 150L);   // ints must be Long
        Map<String, Object> context = Map.of("user", "admin");

        ErrorsOr<Object> result = compiled.executor().execute(input, context);

        if (result.isValue()) {
            System.out.println("Eval result = " + result.getValue().get()); // true
        } else {
            System.out.println("Eval errors: " + result.getErrors());
        }
    }
}  
```
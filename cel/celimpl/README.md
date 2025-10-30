# CEL Implementation Module

## Overview
This module provides the concrete implementation of the CEL (Common Expression Language) core interfaces, including rule builders and executors based on Google's CEL-Java library.

## Components

### DefaultCelRuleBuilder
- **Purpose**: Default implementation of the CEL rule builder interface
- **Location**: `src/main/java/com/hcltech/rmg/celimpl/DefaultCelRuleBuilder.java`
- **Usage**: Provides fluent API for building and compiling CEL rules
- **Tested in**: `CelBuilderTest.java`

### CelRuleBuilders
- **Purpose**: Factory and utility class for creating rule builders
- **Location**: `src/main/java/com/hcltech/rmg/celimpl/CelRuleBuilders.java`
- **Usage**: Provides static factory methods for builder creation

### CelExecutionException
- **Purpose**: Exception thrown during CEL rule execution
- **Location**: `src/main/java/com/hcltech/rmg/celimpl/CelExecutionException.java`
- **Usage**: Indicates errors during rule evaluation
- **Tested in**: `CelExecutionTest.java`

## Key Features

### Fluent Builder API
- Compile CEL expressions into executable programs
- Validate AST, input paths, and context paths
- Inspect which `input.*` and `context.*` fields are referenced
- Safe execution returning results via `ErrorsOr<T>` monad

### Integration with Google CEL-Java
Built on top of [Google's CEL-Java library](https://github.com/google/cel-java), providing:
- Full CEL specification support
- Type-safe expression evaluation
- AST compilation and optimization

## Important Usage Notes

### ⚠️ Type Requirements
- **Use `Long` for integers**: CEL's `int` is 64-bit. Using Java `Integer` will cause overload errors
  ```java
  Map.of("amount", 150L)  // ✓ Correct
  Map.of("amount", 150)   // ✗ Wrong
  ```

### ⚠️ Access Patterns
- **Prefer bracket notation**: Use `input["field"]` instead of `input.field`
  ```cel
  input["amount"] > 100    // ✓ Recommended
  input.amount > 100       // Works but less reliable with maps
  ```

### ⚠️ Supported Types
CEL supports:
- `Boolean`, `Long`, `Double`, `String`
- Nested `Map<String, ?>` and `List<?>`
- **Not supported**: Arbitrary POJOs (must be converted first)

### ⚠️ Null Handling
Guard against null values with presence checks:
```cel
"amount" in input ? input["amount"] > 0 : false
```

## Example Usage

```java
import com.hcltech.rmg.celimpl.CelRuleBuilders;

String rule = """
    context["user"] == "admin" &&
    input["amount"] > 100
    """;

var compiledOr = CelRuleBuilders.celBuilder(rule).compile();

if (compiledOr.isError()) {
    System.out.println("Compile errors: " + compiledOr.getErrors());
    return;
}

var compiled = compiledOr.valueOrThrow();
Map<String, Object> input = Map.of("amount", 150L);
Map<String, Object> context = Map.of("user", "admin");

var result = compiled.executor().execute(input, context);
System.out.println("Result: " + result.getValue().get()); // true
```

## Testing
Comprehensive tests are available in `src/test/java/com/hcltech/rmg/celimpl/`:
- `CelBuilderTest.java` - Builder API tests
- `CelExecutionTest.java` - Execution tests
- `InMemoryCelRuleCacheContractTest.java` - Cache implementation tests

## Dependencies
Refer to `pom.xml` for Maven dependencies, including:
- Google CEL-Java library
- RMG celcore module (interfaces)

## Integration
This module implements the interfaces defined in `cel/celcore` and is used by:
- **validation** module (CEL validation executor)
- **bizlogic** module (CEL inline logic executor)
- Any module requiring CEL expression evaluation

## Additional Documentation
See `src/README.md` for detailed API documentation and advanced usage examples.


# Vectorized Lookups Extension

This extension provides a vectorized implementation of Druid lookups for improved performance.

## Features

### VECTORIZED_LOOKUP SQL Function

The `VECTORIZED_LOOKUP` function provides the same functionality as the standard `LOOKUP` function but uses a vectorized implementation for better performance.

#### Syntax

```sql
VECTORIZED_LOOKUP(expr, lookupName[, replaceMissingValueWith])
```

#### Parameters

- `expr`: The expression to look up in the lookup table
- `lookupName`: The name of the registered lookup table
- `replaceMissingValueWith`: (Optional) The value to return if the lookup key is not found

#### Examples

```sql
-- Basic lookup
SELECT VECTORIZED_LOOKUP(dim1, 'my_lookup') AS lookup_result
FROM druid.foo

-- Lookup with default value
SELECT VECTORIZED_LOOKUP(dim1, 'my_lookup', 'NOT_FOUND') AS lookup_result
FROM druid.foo

-- Lookup in WHERE clause
SELECT COUNT(*)
FROM druid.foo
WHERE VECTORIZED_LOOKUP(dim1, 'my_lookup') = 'expected_value'
```

## Installation

This extension is included in the core Druid distribution. No additional installation is required.

## Configuration

The extension is automatically loaded when the vectorized-lookups extension is enabled. The `VECTORIZED_LOOKUP` function will be available alongside the standard `LOOKUP` function.

## Performance

The vectorized implementation provides improved performance for lookup operations by processing multiple values in batches rather than individually. This is particularly beneficial for queries that perform lookups on large datasets.

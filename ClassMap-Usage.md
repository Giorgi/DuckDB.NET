# ClassMap-based Type-Safe Appender

This implementation provides a type-safe way to append data to DuckDB tables using ClassMap-based mappings.

## Problem Solved

The original issue was that users could accidentally append values with mismatched types (e.g., `decimal` to `REAL` column), causing silent data corruption. The ClassMap approach ensures type safety at compile time.

## How It Works

### 1. Define a ClassMap

Create a ClassMap that defines the property-to-column mappings:

```csharp
public class PersonMap : DuckDBClassMap<Person>
{
    public PersonMap()
    {
        Map(p => p.Id).ToColumn(0);        // Maps to INTEGER column
        Map(p => p.Name).ToColumn(1);      // Maps to VARCHAR column  
        Map(p => p.Height).ToColumn(2);    // Maps to REAL column - enforces float!
        Map(p => p.BirthDate).ToColumn(3); // Maps to TIMESTAMP column
    }
}
```

### 2. Use Type-Safe Appender

```csharp
// Create table
connection.ExecuteNonQuery(
    "CREATE TABLE person(id INTEGER, name VARCHAR, height REAL, birth_date TIMESTAMP)");

// Create data
var people = new[]
{
    new Person { Id = 1, Name = "Alice", Height = 1.65f, BirthDate = new DateTime(1990, 1, 15) },
    new Person { Id = 2, Name = "Bob", Height = 1.80f, BirthDate = new DateTime(1985, 5, 20) },
};

// Use mapped appender - type safety enforced by ClassMap
using (var appender = connection.CreateAppender<Person, PersonMap>("person"))
{
    appender.AppendRecords(people);
}
```

## Benefits

### 1. **Compile-Time Type Safety**
The ClassMap defines the expected types. If your `Person` class has `decimal Height`, you must explicitly map it to the correct DuckDB type, making the type mismatch visible.

### 2. **No Performance Overhead**
Unlike validation in the low-level appender, the ClassMap approach:
- Only validates mappings once when creating the appender
- Uses compiled property getters for fast value extraction
- No per-value type checks during append operations

### 3. **Explicit Type Mapping**
```csharp
// Option 1: Explicit type specification
Map(p => p.Height, DuckDBType.Float).ToColumn(2);

// Option 2: Automatic type inference
Map(p => p.Height).ToColumn(2);  // Infers DuckDBType.Float from float property
```

### 4. **Backward Compatible**
The original fast, low-level `CreateAppender()` API remains unchanged:
```csharp
// Still available for maximum performance when type safety is not needed
using var appender = connection.CreateAppender("myTable");
appender.CreateRow()
    .AppendValue((float?)1.5)  // Manual type control
    .EndRow();
```

## Example: Preventing the Original Issue

### ❌ Before (Silent Corruption)
```csharp
public class MyData
{
    public decimal Value { get; set; }  // Oops! decimal is 16 bytes
}

// This would silently corrupt data
using var appender = connection.CreateAppender("myTable");  // REAL column
appender.CreateRow()
    .AppendValue(data.Value)  // decimal to REAL - CORRUPTION!
    .EndRow();
```

### ✅ After (Type Safety with ClassMap)
```csharp
public class MyData
{
    public float Value { get; set; }  // Correct type!
}

public class MyDataMap : DuckDBClassMap<MyData>
{
    public MyDataMap()
    {
        Map(x => x.Value);  // Automatically maps float to REAL
    }
}

// Type-safe appender prevents mismatches
using var appender = connection.CreateAppender<MyData, MyDataMap>("myTable");
appender.AppendRecords(dataList);  // Safe!
```

If you tried to map a `decimal` property to a `REAL` column, you'd need to explicitly handle the conversion in your ClassMap, making the type mismatch visible.

## API Overview

### Creating Mapped Appenders

```csharp
// Simple table name
var appender = connection.CreateAppender<T, TMap>("tableName");

// With schema
var appender = connection.CreateAppender<T, TMap>("schemaName", "tableName");

// With catalog and schema
var appender = connection.CreateAppender<T, TMap>("catalog", "schema", "table");
```

### Appending Data

```csharp
// Single record
appender.AppendRecord(record);

// Multiple records
appender.AppendRecords(recordList);

// Close and flush
appender.Close();
```

### Automatic Type Inference

The ClassMap automatically infers DuckDB types from .NET types:

| .NET Type | DuckDB Type |
|-----------|-------------|
| `bool` | Boolean |
| `sbyte` | TinyInt |
| `short` | SmallInt |
| `int` | Integer |
| `long` | BigInt |
| `byte` | UnsignedTinyInt |
| `ushort` | UnsignedSmallInt |
| `uint` | UnsignedInteger |
| `ulong` | UnsignedBigInt |
| `float` | Float |
| `double` | Double |
| `decimal` | Decimal |
| `string` | Varchar |
| `DateTime` | Timestamp |
| `DateTimeOffset` | TimestampTz |
| `TimeSpan` | Interval |
| `Guid` | Uuid |
| `DateOnly` | Date |
| `TimeOnly` | Time |

## Performance

- **No runtime overhead**: Type mapping is validated once at appender creation
- **Fast value extraction**: Uses compiled expression getters
- **Same underlying performance**: Uses the same fast data chunk API as the low-level appender

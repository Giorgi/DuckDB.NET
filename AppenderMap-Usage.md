# AppenderMap-based Type-Safe Appender

This implementation provides a type-safe way to append data to DuckDB tables using AppenderMap-based mappings with automatic type validation.

## Problem Solved

The original issue was that users could accidentally append values with mismatched types (e.g., `decimal` to `REAL` column), causing silent data corruption. The AppenderMap approach validates types against actual column types from the database.

## How It Works

### 1. Define an AppenderMap

Create an AppenderMap that defines property mappings in column order:

```csharp
public class PersonMap : DuckDBAppenderMap<Person>
{
    public PersonMap()
    {
        Map(p => p.Id);            // Column 0: INTEGER
        Map(p => p.Name);          // Column 1: VARCHAR  
        Map(p => p.Height);        // Column 2: REAL
        Map(p => p.BirthDate);     // Column 3: TIMESTAMP
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

// Use mapped appender - type validation happens at creation
using (var appender = connection.CreateAppender<Person, PersonMap>("person"))
{
    appender.AppendRecords(people);
}
```

## Benefits

### 1. **Type Validation Against Database Schema**
The mapped appender retrieves actual column types from the database and validates that your .NET types match:
- `int` → `INTEGER` ✅
- `float` → `REAL` ✅  
- `decimal` → `REAL` ❌ Throws exception at creation!

### 2. **No Performance Overhead**
- Type validation happens once when creating the appender
- Uses the same fast data chunk API as the low-level appender
- No per-value type checks during append operations

### 3. **Support for Default and Null Values**
```csharp
public class MyMap : DuckDBAppenderMap<MyData>
{
    public MyMap()
    {
        Map(d => d.Id);
        Map(d => d.Name);
        DefaultValue();  // Use column's default value
        NullValue();     // Insert NULL
    }
}
```

### 4. **Backward Compatible**
The original fast, low-level `CreateAppender()` API remains unchanged:
```csharp
// Still available for maximum performance
using var appender = connection.CreateAppender("myTable");
appender.CreateRow()
    .AppendValue((float?)1.5)
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

### ✅ After (Type Safety with Validation)
```csharp
public class MyData
{
    public float Value { get; set; }  // Correct type!
}

public class MyDataMap : DuckDBClassMap<MyData>
{
    public MyDataMap()
    {
        Map(x => x.Value);  // Validated: float → REAL ✅
    }
}

// Type mismatch detected at appender creation
using var appender = connection.CreateAppender<MyData, MyDataMap>("myTable");
appender.AppendRecords(dataList);  // Safe!
```

If you tried to use a `decimal` property with a `REAL` column:
```csharp
public class WrongMap : DuckDBClassMap<MyData>
{
    public WrongMap()
    {
        Map(x => x.DecimalValue);  // decimal property
    }
}

// Throws: "Type mismatch for property 'DecimalValue': 
//          Property type is Decimal (maps to Decimal) but column 0 is Float"
var appender = connection.CreateAppender<MyData, WrongMap>("myTable");
```

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
// Multiple records
appender.AppendRecords(recordList);

// Close and flush
appender.Close();
```

### Mapping Options

```csharp
public class MyMap : DuckDBClassMap<MyData>
{
    public MyMap()
    {
        Map(x => x.Property1);  // Map to column in sequence
        Map(x => x.Property2);  
        DefaultValue();         // Use column default
        NullValue();           // Insert NULL
    }
}
```

### Type Mappings

The mapper validates .NET types against DuckDB column types:

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

- **No runtime overhead**: Type mapping validated once at appender creation
- **Fast value extraction**: Uses compiled expression getters
- **Same underlying performance**: Uses the same fast data chunk API as the low-level appender
- **Type safety without cost**: Validation at creation, not per-value

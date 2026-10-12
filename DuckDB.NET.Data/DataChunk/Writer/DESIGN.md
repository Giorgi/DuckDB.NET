# Design of the vector writers

This file records why the write path is written the way it is: the writers in this folder, and the code that
calls them (the appender row, the mapped appender, and the loops of scalar and table functions). Each decision
says what was done, why, what was measured, and what was tried and rejected. The read path has its own file,
`../Reader/DESIGN.md`.

The numbers are a snapshot. They were measured on one machine in October 2026 with BenchmarkDotNet, each build in
its own process, against a published build as the baseline. A ratio below 1 means faster. Measure again before
relying on them; the ratios should hold, the absolute times will not.

## How a value is written

```
DuckDBAppenderRow.AppendValue(int?)                        the appender; the mapped appender calls this too
  -> AppendValueInternalStruct<int>(value)                 NULL check
    -> GetCurrentWriter()                                  the writer of the current column
      -> VectorDataWriterBase.WriteValue<int>(value, row)
           a switch on the type of the value               the JIT keeps one arm for a value type
             -> AppendNumeric(int, ulong)                  a plain virtual call
                  -> store the value in the vector
```

- **A function result** takes the same path from `WriteValue<T>` on. The loop of a typed function calls it on
  the class. A callback written against `IDuckDBDataWriter` calls it through the interface.
- **A list or an array** is written by `CollectionVectorDataWriter`, which calls `WriteValue<T>` of the writer of
  its items for every item.
- **A parameter of a command does not use the writers.** `ClrToDuckDBConverter` makes a DuckDB value for it. The
  two share the conversion of dates and times (decision 6).

The cost that most decisions here avoid is the one the read path has: a virtual method with its own type
parameter, or a generic method called through an interface, is looked up at run time on every call. See "The cost
everything here avoids" in `../Reader/DESIGN.md`.

## Decisions

### 1. A numeric value is written through a plain virtual method

- **What:** `VectorDataWriterBase` has ten overloads, `AppendNumeric(int, ulong)` and so on, one per numeric
  type. The base class throws; `NumericVectorDataWriter` overrides each with a store. (`f68a0f7`)
- **Why:** it was one method, `AppendNumeric<T>`, virtual and generic, overridden by one class with one line.
  Every value paid the lookup around a store of a few bytes.
- **Measured:** `AppendIntegers` 0.70 to 0.74 against `1.5.6` (13.6 ns per value against 18.4), mixed numeric
  types 0.75 to 0.86.
- **Rejected:** a non-virtual method with a flag set by the constructor ("this writer takes numeric values"). It
  measured the same within the noise and was dropped as less clear.
- **Still generic and virtual:** `AppendEnum<TEnum>`.

### 2. The appender row checks the column and gets the writer in one step

- **What:** `DuckDBAppenderRow.GetCurrentWriter()` compares the column index with the number of writers,
  unsigned, and returns the writer. The message and the throw are in `ThrowColumnOutOfRange()`. (`241a35c`)
- **Why:** the check was a method of its own with the interpolated message in it, so it was a call for every
  value, and the array access after it had a second bounds check.
- **Measured:** `AppendValueInternalStruct<int>` 94 bytes of code instead of 119, with no call for the check;
  5% to 9% on integer appends.

### 3. `WriteValue<T>` is a call, and stays one

The JIT decides on inlining from the method as written, a switch with about 25 arms, before it folds the switch
to one arm. So it does not inline it. Removing the call is worth 0.5 to 1 ns per value. Three ways were tried:

| Tried | Result |
|---|---|
| `AggressiveInlining` on `WriteValue<T>` | Inlines for a value type. For a reference type nothing folds, and the whole chain of type tests, about 1,840 bytes, is copied into every caller. |
| Inline for value types only, through a `typeof(T).IsValueType` test | Works on .NET 10. .NET 8 refuses ("inline exceeds budget"), and a string then goes through two calls more than before. |
| A separate typed path for numeric values (`typeof(T) == typeof(int)` tests, no switch) | Inlines on both runtimes. Measured 3% to 5% on a table of integers and nothing on mixed types. Not applied: a second write path next to `WriteValue<T>` for that gain. |

Do not try a fourth without the previous build as the baseline in the same run. A step of 1 ns does not show
between two runs.

### 4. A nullable property of the mapped appender is not boxed

- **What:** `PropertyMapping<T, TProperty>.AppendToRow` tests `typeof(TProperty).IsValueType && default(TProperty)
  is null`, both constants for the JIT, and passes a nullable value on with one line per type:
  `if (typeof(TProperty) == typeof(int?)) row.AppendValue(Unsafe.As<TProperty, int?>(ref value))`. (`56c6f90`)
- **Why:** the method chose the `AppendValue` overload with a pattern switch. For `TProperty = int?` each type
  test of the switch boxes the value.
- **Measured**, eight `int?` properties, per value: 132 ns and 72 bytes to 24 ns and none on .NET 8; 21.9 ns and
  24 bytes to 19.0 ns and none on .NET 10. A nullable property now costs what a plain one does.
- **`WriteValue<T>` had the same boxing** for a nullable `T`, which the appender row does not reach. Decision 9
  removes it in the same way.

### 5. The date and time structs have their own equality

- **What:** `DuckDBDateOnly`, `DuckDBTimeOnly` and `DuckDBTimestamp` implement `IEquatable<T>` and override
  `Equals` and `GetHashCode`. (`864e470`)
- **Why:** `IsPositiveInfinity` and `IsNegativeInfinity` call `Equals`. Without an override that is
  `ValueType.Equals(object)`, which boxes the argument and, because the structs have padding, compares the fields
  through reflection. Every date or timestamp written as one of these structs allocated 432 to 752 bytes.
- **Measured:** 0.08 to 0.10 of the time for appends of `DateTime`, `DateTimeOffset` and `DateOnly`, and 432 to
  752 bytes per value down to none.

### 6. A .NET date or time is converted by arithmetic

- **What:** a `DateTime`, `DateTimeOffset`, `DateOnly` or `TimeOnly` is converted to what DuckDB stores in
  `Extensions/DateTimeExtensions.cs`: a subtraction and a division of the ticks. Writers and parameters both call
  it. The epoch and the layout of a TIMETZ are constants there, one copy for reading and writing. (`0407f4c`)
- **Why:** the value was taken apart into year, month, day, hour, minute, second and microsecond, and DuckDB put
  the parts together again (`duckdb_to_date`, `duckdb_to_timestamp`, `duckdb_to_time`, `duckdb_create_time_tz`).
- **The stored values did not change,** including the ones that look wrong; see "Behaviour" below. What is below
  a microsecond is dropped toward the earlier microsecond, before 1970 as after it: the ticks from 0001-01-01 and
  the ticks of the epoch are each divided by ten. Dividing the difference would round a value before 1970 up.
- **What still calls DuckDB:** a `DuckDBDateOnly`, `DuckDBTimeOnly` or `DuckDBTimestamp`, which holds the parts.
- **The guard, because this code assumes how DuckDB stores these types:**
  `DuckDB.NET.Test/TemporalWriteConversionTests.cs` compares the arithmetic with DuckDB's own four functions, for
  every day a `DateOnly` holds and every second of the day, and has DuckDB split or print what was written. A
  change of a constant changes reads and writes together, so a round trip does not show it. Do not replace those
  comparisons with constants worked out in C#.
- **Checked:** the build before and the build after stored the same for 300,495 values through the appender and
  4,495 through parameters, function results and lists; a second reviewer compared the old and the new conversion
  137 million times, with no difference; 26 of 27 deliberate mistakes were caught by the guard.
- **Measured** against the build before, 100,000 rows of four columns: `DateTimeOffset` to TIMESTAMPTZ 0.35 to
  0.45, `DateTime` to DATE 0.44 to 0.51, `DateTime` to TIMESTAMP 0.35 on .NET 8. On .NET 10 that last case ran at
  the old speed for the first third of its measured iterations and gave 0.66 and 0.75 in two runs; with tiered
  compilation off it gave 0.33.
- **A `DateOnly` or `TimeOnly` appended to a row does not reach this code.** `IDuckDBAppenderRow` has no overload
  for them, so they become `DuckDBDateOnly` and `DuckDBTimeOnly`. Adding the overloads makes a call with a
  non-nullable argument ambiguous, so it waits for a major version.

### 7. A HUGEINT is built from a `BigInteger` through `Int128`

- **What:** `new DuckDBHugeInt(BigInteger)` converts to `Int128` and takes its two halves; `DuckDBUHugeInt` does
  the same with `UInt128`. The limits are `Int128.MinValue` and so on. (`dd5bb8a`)
- **Why:** the constructor divided by 2^64 with `BigInteger.DivRem` and corrected both halves for a negative
  value. The limits were parsed from 39-digit strings in the static initializer.
- **Kept:** the range check in front, with `ArgumentOutOfRangeException` and its message. The conversion by itself
  throws `OverflowException`.
- **Measured:** appends of `BigInteger` 0.24 to 0.59 of the time, and 15 to 79 bytes per value down to none.
- **The tests compare the two halves with the ones division gives.** A round trip through `ToBigInteger()` would
  not notice a mistake, because both directions use `Int128`.

### 8. The loop of a typed function calls the reader and the writer as classes

- **What:** `RegisterScalarMethod` takes an action over `VectorDataReaderBase[]` and `VectorDataWriterBase`. A
  typed registration, `RegisterScalarFunction<int, int>("f", x => x + 1)`, has a loop of its own for its number
  of parameters and calls `GetValue<T>` and `WriteValue<T>` on the classes. The public overloads that take a
  callback over the interfaces pass it on unchanged: an action over the interfaces is an action over the classes.
  (`ee223da`)
- **Why:** the typed forms were built on the public overloads, so their loops read and wrote through
  `IDuckDBDataReader` and `IDuckDBDataWriter`, with a lookup for every argument and every result. A shared
  wrapper added a delegate call and a list indexer call per row.
- **Rejected:** a cast back to the classes inside the typed forms. It gives the same loop, and depends on a fact
  that no signature states.
- **The compiled loop** of an `int` function (.NET 10): the read and the user's function are inlined, the second
  behind a type check, and one direct call is left, to `WriteValue<int>` (decision 3).
- **Table functions:** the method compiled from the projection tests the writer of each column for
  `VectorDataWriterBase` and calls its `WriteValue<T>`; any other writer takes the call through the interface. A
  column that the query does not read has a `NullDataWriter`, which is not a `VectorDataWriterBase`. The value of
  such a column is still worked out, once per row, as before.
- **Measured** against `1.5.6-alpha.66`, 1,000,000 rows, one thread, .NET 10:

  | Function | Ratio |
  |---|---|
  | `int` to `int` | 0.30 (11.8 to 3.6 ms) |
  | Two `int` arguments | 0.28 |
  | `int?` to `int?` | 0.73 |
  | `string` to `int` | 0.78 |
  | Table function, four columns | 0.79 (41.6 to 32.8 ms) |
  | The same `int` function written against the interfaces (control) | 1.00 (11.0 ms) |
  | The same sum in SQL, no function (control) | 0.95 (1.0 ms) |

  A typed `int` function costs about 2.6 ns per value more than the sum in SQL, where it cost about 10.7 ns.
- **.NET 8 has no ratio that can be trusted.** The typed `int` function went from 24.0 to 4.9 ms, so there is a
  gain. But the machine was not steady: the two controls measured 0.74 and 0.64 in one run, and 1.17 and 1.24 in
  a second run that was stopped part way.
- **Checked:** the build before and the build after printed the same 9,701 lines for a probe of registrations,
  results, exceptions and the number of calls, on both runtimes.
- **A scalar function gives DuckDB a callback that copies its bind data** (`fc26d12`). The bind data is the ID of
  the connection, which the function needs to keep an exception for the caller. DuckDB copies a bound expression,
  for a filter for example, and a copy without that callback has no bind data.

### 9. A nullable value is written without boxing

- **What:** `WriteValue<T>` tests `typeof(T).IsValueType && default(T) is null` before its switch. Both are
  constants for the JIT, so only a nullable value type keeps the branch. It calls `WriteNullable`, which has one
  line per type, `if (typeof(T) == typeof(int?)) AppendNumeric(Unsafe.As<T, int?>(ref value).GetValueOrDefault(),
  rowIndex)`, in the order of the switch. (`40b625b`)
- **Why:** the switch tests the type of the value with patterns (`int val => ...`). For `T = int?` such a test
  boxes the value. The appender row unwraps a nullable before it calls `WriteValue<T>`, so it never got here.
  Three callers did: a function that returns a nullable value, a nullable column of a table function, and the
  collection writer for the items of a `List<int?>` or an `int?[]`.
- **The compiled code** of `WriteValue<int?>` (.NET 10) has no call to a box or unbox helper. It calls
  `WriteNullable<int?>`, 47 bytes, in which the 21 type tests have folded to one virtual call,
  `AppendNumeric(int)`. `WriteNullable` is not inlined, so a nullable write makes one call more than a plain one.
- **What is still boxed:** a nullable enum, and a nullable struct that has no line. They take the last branch of
  `WriteNullable`, which is the rest of the switch: an enum, a collection, and the error.
- **Measured** on .NET 10 against the build before, which has decision 8:

  | Write | Ratio | Allocated |
  |---|---|---|
  | Function `int?` to `int?`, 1,000,000 rows | 0.61 (22.6 to 13.9 ms) | 21,199 KB to 104 KB |
  | Table function, four columns, one of them `int?` | 0.77 (46.4 to 36.0 ms) | 21,335 KB to 240 KB |
  | `List<int?>` of ten items, 100,000 lists | 0.74 | 21,103 KB to 9 KB |
  | `int?[]` of ten items | 0.70 | 21,103 KB to 9 KB |
  | `List<double?>` of ten items | 0.84 | 21,103 KB to 9 KB |
  | Function `int` to `int`, and `List<int>` (controls) | 1.00 and 0.97 | The same |

  The three list rows are from a run on a machine that was not quiet (`RatioSD` 0.15 to 0.33).
- **Decisions 8 and 9 together,** against `1.5.6-alpha.66`: the `int` function 0.36, the `int?` function 0.40, the
  table function 0.62, with the controls at 0.99 and 1.00. .NET 8 was not measured for this decision.
- **Pinned by:** `SimplifiedScalarFunctionReturnsNullableOfEveryType`, a function for each of the 21 types, and
  `NullableItemsAppendedAfterAFailedListReplaceItsNulls`, for a value written where a NULL was.

## Costs that are known and not yet removed

- **A callback written against the interfaces pays the lookups of decision 8:** 11.0 ms where the typed form of
  the same function takes 3.6 ms. Non-generic methods on the interfaces would remove them.
- **A typed function reads a nullable argument through a delegate and tests it for NULL twice.** After decision 9
  an `int?` function takes 13.9 ms where an `int` function takes 6.2 ms.
- **A list is walked through its enumerator:** one allocated for every list, and two interface calls per item.
  The collection is also tested against about 45 interfaces, for every list, to find its item type.
- **A C# enum is boxed twice** on its way into an ENUM column, 48 bytes per value, and `AppendEnum<TEnum>` is
  looked up at run time.
- **A table function gets its rows as `object`:** a row that is a value type is boxed, and every row costs two
  interface calls on the enumerator and a delegate call.
- **A `DateOnly` or `TimeOnly` appended to a row** goes through the DuckDB struct (decision 6).
- **Every chained `AppendValue` is a call through `IDuckDBAppenderRow`,** because the fluent methods return the
  interface.
- **A `decimal` in a column of more than 18 digits** allocates 32 bytes per value, and a list 80 to 100 bytes
  per list. Neither has been looked at.

## Behaviour that is kept on purpose

- **The appender does no type checking.** It writes the bytes of the .NET type it is given. `AppendValue(1)`
  into an INTEGER column stores garbage, and a `DateTime` appended to a TIME column stores the microseconds of
  the timestamp. The mapped appender checks types when it is created.
- **A TIMESTAMP_S and a TIMESTAMP_MS round toward zero before 1970,** so 1969-12-31 23:59:59.9999999 is stored
  as 1970-01-01 00:00:00. DuckDB's own cast rounds down.
- **A `DateTime` outside 1677 to 2262 wraps around in a TIMESTAMP_NS,** with no error.
- **A typed scalar function with no nullable parameter is called for a NULL in a column.** DuckDB answers a
  constant NULL itself; for a column it passes every row. A value-type parameter then fails with "value is null"
  and a `string` parameter gets `null`.

## Rules for changing this code

1. Do not add a virtual method with its own type parameter to the write path, and do not call a generic method
   through an interface in a loop.
2. Test a type parameter as `typeof(T) == typeof(X)`, or `typeof(T).IsValueType`, with nothing else in the
   condition. The JIT folds those; it does not fold a pattern test of a nullable value, which boxes it.
3. Put a message and its `throw` in a method of their own, as `ThrowColumnOutOfRange` is.
4. A conversion must store what it stored before. Compare the build before with the build after on real values,
   and keep the tests that compare with DuckDB.
5. Measure against the previous build in the same run, with a case that must not change as the control, and on
   both target frameworks. If the control moves, the run is not usable.
6. Take allocation figures from a Release build. A Debug build boxes every value once in `WriteValue<T>`, so a
   test that limits allocation has to leave room for that.
7. Read the compiled code (`DOTNET_JitDisasm`) for a change on the write path.

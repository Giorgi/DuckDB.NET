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
- **A C# enum** does not go through `WriteValue<T>`: the appender row reads its number and calls `AppendEnum` of
  the writer (decision 15).
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
- **`AppendEnum<TEnum>` was the last generic virtual method of the writers.** Decision 15 replaced it.

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
- **A `DateOnly` or `TimeOnly` appended to a row reaches this code since 2.0.** Before, `IDuckDBAppenderRow` had
  no overload for them, so they became `DuckDBDateOnly` and `DuckDBTimeOnly`. Each of the two pairs converts
  implicitly both ways, so the nullable overloads alone make a call with a non-nullable argument ambiguous. The
  interface therefore also has an overload for each of the four types that is not nullable: an exact match
  always wins, and both implicit conversions are kept. Measured with `DateAppenderBenchmark` against the
  1.5.6.1 package, .NET 10, one run, 1,000,000 rows of four columns: `DateOnly` to DATE 0.40, `TimeOnly` to
  TIME 0.59, `DateTime` to DATE (the control, which took the arithmetic path in both builds) 0.89, with a
  standard deviation of 15% to 20% of the mean in every job.

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
- **Table functions:** the method compiled from the projection tested the writer of each column for
  `VectorDataWriterBase` and called its `WriteValue<T>`; any other writer took the call through the interface,
  as the writer of a column that the query does not read did. Decision 14 replaced that method.
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

### 10. A decimal is written by integer arithmetic, and one that is too wide is refused

- **What:** a `decimal` is an integer of 96 bits, a sign and a scale. `DecimalVectorDataWriter` takes those parts
  out (`GetParts`, one call to `decimal.GetBits`), multiplies or divides the integer by a power of ten to get
  the integer for the scale of the column, and stores it. It is the reader's conversion in the other direction.
- **Two methods.** `AppendDecimal` is for an integer that fits 64 bits (every value of up to 19 digits) whose
  scale is less than 20 away from the scale of the column: one 64-bit multiplication (`Math.BigMul`) or
  division, for every storage size. `AppendLongDecimal` does the same steps on `UInt128` for the rest: a longer
  integer, such as the 28 digits of `1m / 7m`, or `1m` into a DECIMAL(38,20). It is a method of its own for the
  reason the reader has `GetWideDecimal`: 128-bit arithmetic needs a large stack frame, and in the first method
  that frame was reserved and partly cleared for every value: 200 bytes reserved and 80 cleared, against 72 and
  16 now (.NET 10). `AppendDecimal` compiles to 564 bytes with one `mulx` and one `div`, and its only calls are
  to the long method and to the exception.
- **Why:** every value was multiplied with `decimal.Multiply` and then cast, and a column of more than 18 digits
  went through `BigInteger`, which allocated for the value and for the power of ten.
- **A value with more digits than the width of the column is refused** with `OverflowException`, which names the
  value and the column. DuckDB does not check a chunk that is appended. Until this change the writer checked only
  that the integer fit the storage size, so `10000000.00` went into a DECIMAL(9,2) and `1` into a
  DECIMAL(38,38), and DuckDB printed garbage for them.
- **The check is also what keeps the long method short.** A value fits when the digits in front of its point
  fit, which is one comparison on the decimal, made first. A value that passes is below 10^38 when it is
  rescaled, so the 128-bit multiplication after it cannot overflow and needs no guard.
- **Rejected, each built and measured:**
  - everything through `UInt128` in one method: correct, half the old time, and the frame above on every value;
  - a third method, the old code, kept to throw the old exceptions. It was needed only while a value that did not
    fit still had to fail in the old way. With the width check there is one exception, thrown here;
  - the old code for long values, to save the 128-bit method: long values stayed at the old speed and kept
    allocating in a wide column.
- **Kept:** digits after the point that the column has no room for are dropped, toward zero. DuckDB rounds them
  when it casts a parameter. That difference is deliberate; `DecimalsTruncateExcessScale` pins it.
- **Parameters start from the same parts** (`DecimalToDuckDBValue`), and pass the exact number of digits as the
  width. The earlier count, from `BigInteger.Log10`, was wrong near a power of ten: it took 1000 for three
  digits. DuckDB then stored a value one digit too long for its column, which it refuses now.
- **Checked:** 96,570 decimals into 15 column types, with the build before and with this one, by DuckDB's
  `hash()` of what was stored, on both runtimes. Every value that fits was stored as before (1,055,860), every
  one that does not is refused (392,690, of which 22,275 used to be stored), and none is on the wrong side.
  What fits was worked out apart from the code, with exact arithmetic. Not as text: DuckDB prints a decimal
  that is too wide differently from one run to the next.
- **Measured** against the build before, 100,000 rows of four columns, with the BIGINT control at 1.01 on .NET 10
  and 1.05 on .NET 8:

  | Write | .NET 10 | .NET 8 | Allocated |
  |---|---|---|---|
  | The scale of the column, DECIMAL(18,4) | 0.49 | 0.56 | 11.9 KB to 14.9 KB |
  | Fewer digits after the point than the column | 0.44 | 0.60 | 11.9 KB to 14.9 KB |
  | DECIMAL(9,2) | 0.46 | 0.53 | 11.9 KB to 14.9 KB |
  | DECIMAL(38,10) | 0.50 | 0.40 | 12,512 KB to 14.9 KB |
  | 28 digits into DECIMAL(18,4) | 0.81 | 0.81 | 11.9 KB to 14.9 KB |
  | 28 digits into DECIMAL(38,10) | 0.37 | 0.52 | 25,894 KB to 14.9 KB |

  An earlier run on .NET 10 gave the same ratios within 0.05.

  The 3 KB more is 3,136 bytes, which equals 16 bytes, the two fields of the limit, for each of the four writers
  that the appender creates again for each of the 49 chunks. Where the bytes go was not measured.

  A parameter shows no change that can be measured, on either runtime: 0.90 to 1.02 for four decimals per
  execution and for a list of 10,000 decimals, in runs where four BIGINT parameters, the control, gave 0.95 and
  1.05, and the same allocation. An execution costs about 450 µs, and the conversion is a small part of it.

### 11. A string is encoded by the writer, and a short one is written into the vector

- **What:** `StringVectorDataWriter.AppendString` encodes the string to UTF-8 itself. A string of up to 12 bytes
  is written straight into the vector, in the layout of DuckDB's `string_t`: the length, then 12 bytes with zeros
  after the string. No native call is made for it. A longer one is encoded on the stack, or into a rented array
  above 170 characters, and handed to `duckdb_unsafe_vector_assign_string_element_len` with its length.
  (`4ef7efb`)
- **Why:** the string was passed to `duckdb_vector_assign_string_element`. The marshaller of the call encoded it
  with a terminator, into a buffer on the stack or, for a long string, on the heap. DuckDB then measured it
  again and checked the UTF-8.
- **The check of the UTF-8 is skipped** because what the .NET encoder writes is always valid: a surrogate
  without its pair becomes the replacement character.
- **The short string uses `DuckDBStringInlined`,** the struct the reader's `DuckDBString` is made of, so the
  layout is declared once for both directions. The bytes after the string have to be zero: DuckDB compares and
  hashes a short string by its 16 bytes.
- **Kept:** a string ends at its first NUL character, as it did when DuckDB measured it as a C string. A
  parameter does the same.
- **Checked:** 27,016 strings appended to a VARCHAR, a VARCHAR[] and a BLOB column and returned from a scalar
  function are stored byte for byte as before, on both runtimes. `StringWriteConversionTests` guards the layout
  by comparing with the bytes DuckDB reads back and with strings DuckDB made.
- **Measured** against the build before, 100,000 rows of four columns, two runs on each runtime. The BIGINT
  control was between 0.84 and 1.16 in these runs, so a ratio is good to about a tenth:

  | Strings | .NET 10 | .NET 8 |
  |---|---|---|
  | 5 to 8 ASCII characters | 0.69, 0.77 | 0.51, 0.73 |
  | 12 ASCII characters | 0.72, 0.74 | 0.58, 0.67 |
  | 40 ASCII characters | 0.90, 0.94 | 0.87, 0.93 |
  | 300 ASCII characters | 0.81, 0.80 | 0.82, 0.89 |
  | 4 Georgian characters, 12 bytes | 0.67, 0.74 | 0.65, 0.68 |
  | 30 Georgian characters, 90 bytes | 0.59, 0.59 | 0.60, 0.55 |

  Nothing is allocated on the managed heap, before or after. Why text that is not ASCII gains most was not
  measured; DuckDB's check of the UTF-8, which is no longer made, is the likely part.

### 12. The items of a `List<T>` and of an array are walked as a span

- **What:** `CollectionVectorDataWriter.WriteItems<T>` takes a `List<T>` (`CollectionsMarshal.AsSpan`) and a
  `T[]` as a read-only span and writes the items in a `for` loop. Every other collection keeps the `foreach`
  over `IEnumerable<T>`. (`69e1205`)
- **Why, and on which runtime:** .NET 8 allocates the enumerator for every list. .NET 10 removes it by itself,
  but only while `WriteItems<T>` sees one type of collection for a `T`. A process that appends a `List<int>`, an
  `int[]` and a `Collection<int>` allocated 40 bytes per list on .NET 10 too, until this change. So the gain is
  on .NET 8, and on .NET 10 for a mix of collections. That is why the code is not under `#if`.
- **Read in the compiled code:** on .NET 10 the final code of the loop before the change has the enumerator of
  the list inlined and no allocation; on .NET 8 it calls the allocation helper. The loop over the span is 72
  bytes and calls `WriteValue<T>` once per item.
- **Measured** against the build before, 100,000 lists of ten items:

  | | .NET 8 | .NET 10 |
  |---|---|---|
  | Allocated, `List<int>` and `List<int?>` | 3,915 KB to 8.6 KB | 8.65 KB, the same |
  | Allocated, `int?[]` | 3,134 KB to 8.6 KB | the same |
  | Allocated, `List<double?>` | 4,696 KB to 8.6 KB | the same |
  | Time, `List<int?>` | 0.78 and 0.79 in two runs | no change |
  | Time, the other three | 0.83 to 1.29, not the same twice | no change |

- **A read-only span** because it also takes an array of a derived type, such as a `string[]` that arrives as
  items of `object`. A writable span would throw there.
- **Changed with it:** a `List<T>` that is modified while it is appended no longer throws.
- **Not done:** the outer list of a `List<List<int>>` still goes through its enumerator, because it arrives as
  items of `object`.

### 13. The mapped appender gives its mappings the row as its class

- **What:** `IPropertyMapping<T>.AppendToRow` takes `DuckDBAppenderRow`, the sealed class, where it took
  `IDuckDBAppenderRow`. The mappings are an array walked by index, where they were a `List` walked with
  `foreach`. (`c7da3b7`)
- **Why:** every value was appended through the interface, one interface call per value.
- **A `DateOnly` or a `TimeOnly` property now reaches the arithmetic of decision 6.** The class has
  `AppendValue(DateOnly?)` and `AppendValue(TimeOnly?)`, which the interface lacked until 2.0, and the mappings
  call them. A property of the DuckDB types is written as before. Since 2.0 a value that is not nullable goes to
  the overload of its exact type.
- **Rejected: one method compiled for the map from an expression tree.** Built and measured (branch
  `mapped-appender`). It was ahead only for nullable properties on .NET 8 (0.66 and 0.70 against 0.85 and
  0.81), level or behind elsewhere, doubled what an appender allocates when it is created, and needs
  `Expression.Compile`, which runs interpreted under Native AOT.
- **Measured** against the build before, 100,000 records, two runs, before the change for `DateOnly` and
  `TimeOnly`, which was not measured. The BIGINT control moved between 0.78 and 1.20, so these are indications:

  | Record | .NET 10 | .NET 8 |
  |---|---|---|
  | 8 `int` properties | 0.87, 0.87 | 0.83, 0.89 |
  | 8 `int?` properties | 0.85, 0.91 | 0.85, 0.81 |
  | 5 properties of different types | 0.92, 0.92 | 0.94, 0.91 |

- **The cast of the row is inside the call.** Read in the compiled code: .NET 10 moves it out of the loop, and
  .NET 8 tests the type once per property, a compare and a branch.
- **Left:** two indirect calls per value, to the mapping and to the getter. The getter is a `Func`, so it cannot
  be made part of a compiled method without a change of `Map`.

### 14. A table function is read by a scan that knows the type of its rows and the columns of the query

- **What, in two steps.**
  1. *The rows keep their type.* A scan is a `TableFunctionScan`: the rows and the method that
     writes one, both typed. The callback that DuckDB calls is static and cannot be generic, so it sees the
     non-generic base and calls `Fill` once for a chunk. The type comes from `RegisterTableFunction<TData, ...>`,
     which stores a factory for the scan in the extra info of the function. An array and a `List<T>` are read by
     position; any other source as `IEnumerator<TData>`. The types of the columns are created once for a scan.
     A list that has another length than when the scan started is refused, before every chunk and after it:
     its enumerator refused a list that was changed, and read by position a row that is added or removed would
     shift the rows that follow without a word. After the chunk too, because a query with a `LIMIT` may not ask
     for another one. A change that keeps the length is not noticed.
  2. *The method that writes a row is compiled for the columns the query reads.* DuckDB says which
     those are when it starts a scan. The method is compiled then and kept for the function by its list of
     columns. A scan gets the writers of those columns only, in the order of the chunk.
- **Why:** a scan held a non-generic `IEnumerator` and a method that took an `object`. Every row was fetched as
  an object and cast back, and a row that is a struct was boxed. The method that writes a row was compiled once
  for every column, so it tested the writer of each column for every row, and the value of a column that the
  query does not read was still worked out and handed to a writer that does nothing, through the interface.
- **Read in the compiled code** (.NET 10; .NET 8 has the same shape):
  - the method that writes a row was 449 bytes for four columns, with two calls for a column: a helper of the
    runtime for the type test, because the writers are classes derived from `VectorDataWriterBase`, and
    `WriteValue<int>`. It is 125 bytes now, one call for a column, and 39 bytes when the query reads one column;
  - `Fill` for an array is 148 bytes: load the row, call the method that writes it, count.
- **Measured** against the commit before, a million rows, on the performance cores, with the same query over a
  table as the control (0.99 and 1.03 on .NET 10; 1.00 and 0.84 on .NET 8):

  | Query | .NET 10 | .NET 8 |
  |---|---|---|
  | One column of a function that has eight | 0.07 (62.1 to 4.5 ns a row) | 0.07 (61.1 to 4.6 ns) |
  | Rows of a class from an array, one column | 0.38 | 0.43 |
  | The same, four columns | 0.47 | 0.43 |
  | The same rows from an iterator, one column | 0.50 | 0.67 |
  | The same, four columns | 0.59 | 0.54 |
  | Rows that are a struct, one column | 0.23 | 0.12 |
  | The same, four columns | 0.43 | 0.26 |

  Rows that are a struct allocated 47 MB for a million rows and allocate 0.1 MB. Step 1 alone, .NET 10: 0.51,
  0.60 and 0.31 for one column, and 0.90, 0.94 and 0.67 for four, where the method that writes a row was most
  of the time.
- **What a row costs now:** about 4.5 ns for one column and 11.5 to 13 ns for four, on both runtimes. The same
  query over a table costs 0.8 and 2.6 ns.
- **Changed with it:** the expression of a column that the query does not read is not evaluated. It was
  evaluated once for a row, and its value thrown away.
- **Not changed:** the registration with a method that takes an `object` and an array of writers. That method
  chooses a writer by the index of the column, so it still gets a writer for every column, and the
  `NullDataWriter` for the ones the query does not read (`ObjectTableFunctionScan`).
- **Rejected, each built and measured:**
  - the method compiled once, with a test of the type of the writer that does nothing in place of the test for
    `VectorDataWriterBase`. The expression compiler turned the test into a call of `Object.GetType`: no gain;
  - the same with a comparison against the one instance of that writer: 0.54 for four columns. The method
    compiled for the columns of the query replaced it: no test at all, and nothing for a column that is not
    read.
- **Tell DuckDB the length of an array.** When the data of a function is an array and the function gives no
  `CardinalityHint`, its length is passed on. Only for an array, whose length cannot change:
  `duckdb_bind_set_cardinality` sets the number as the most rows the function can return when `is_exact` is
  false, and as an estimate only when it is true (read at v1.5.6), the reverse of what the names say. DuckDB
  then adds integers without a check for overflow when that many rows cannot overflow, so a function that
  returns more rows than it said gets a wrong sum: 3 rows of 4,000,000,000,000,000,000 after a count of 2 gave
  -6,446,744,073,709,551,616. DuckDB 2.0 has the two the right way round (commit `90cb1904ca5` of DuckDB, read
  on the `v2.0-cyanoptera` branch), and there a maximum that is too small also removes a `LIMIT` that is larger
  (read, not run). The length of an array is true either way.
- **A `CardinalityHint` of a function has the same trap** with `IsExact: false` on DuckDB 1.5. It is passed on
  as it is and not swapped: the comment on the type says what each value does, and 2.0 corrects it.
- **Pinned by:** `RegisterTableFunctionSimplifiedReadsEverySourceTheSameWay` (an array, a `List`, an iterator
  and a set; a class and a struct; three chunks; two scans in one query),
  `RegisterTableFunctionSimplifiedReadsEachColumnOnceForARow` (no value is asked for a column that is not read,
  and each set of columns has its own method), `RegisterTableFunctionSimplifiedRefusesAListThatChangesLength`,
  and `RegisterTableFunctionOverAnArray_LengthAppearsInQueryPlan`.
- **Under Native AOT** the compiled methods are interpreted, as the one compiled at registration was. The
  registration with a method that takes an `object` uses no expression tree.

### 15. A C# enum is appended as its number, without a box

- **What:** `DuckDBAppenderRow.AppendValue<TEnum>` reads the number of the enum value itself, with
  `EnumValue<TEnum>.ToUInt64`, and calls `AppendEnum(ulong, ulong)` of the writer and then `MarkValid`, which
  `WriteValue<T>` would have called. `AppendEnum<TEnum>(TEnum, ulong)`, generic and virtual, became
  `AppendEnum(ulong, ulong)`. When `TEnum` is `System.Enum` itself, the value is already boxed and goes through
  `WriteValue<T>` as before; so does a boxed enum anywhere else, converted by `EnumValue.ToUInt64(Enum)`, the
  code that was in `EnumVectorDataWriter`.
- **Why:** an enum went through the switch of `WriteValue<T>`, which tested it against `Enum` and boxed it. Then
  `AppendEnum<TEnum>` was looked up at run time, and ran as shared generic code, with another lookup. It read the
  number with `Enum.GetTypeCode`, `Enum.GetValue`, which boxes again, and `Convert`, through `IConvertible`. Two
  boxes, 48 bytes, for every value.
- **How the number is read:** `EnumValue<TEnum>` keeps the type code of the enum in a static readonly field. At
  Tier 1 a static readonly field of a class that is already initialized is a constant, so one line of the switch
  is left: a load, `movzx` or `movsx`. A signed number is widened as a cast widens it, so a negative value is a
  number past the last name and is refused, as it was. `ToUInt64` is `AggressiveInlining`: the JIT judges inlining
  by the switch as written, before it folds it, and called a method of two instructions for every value.
- **Why in the appender row and not in `WriteValue<T>`:** it was built first in `WriteValue<T>`, with a test of
  `typeof(T).IsEnum`, which both runtimes fold (read in the compiled code). The maintainer asked to keep code for
  enums out of `VectorDataWriterBase`. The appender row is the only caller with an enum type in hand: a scalar
  function, a table function and the mapped appender have no path for enums. What it cost: `MarkValid` is
  internal, and an enum that reaches `WriteValue<T>` some other way would be boxed.
- **Read in the compiled code** (Tier 1): `AppendValue<TEnum>` is 161 bytes on .NET 10 and 172 on .NET 8, with one
  call, to `EnumVectorDataWriter.AppendEnum`, guessed by the JIT with the virtual call as the fallback, and
  `MarkValid` inlined. Before, `WriteValue<TEnum>` called the allocation helper and `CORINFO_HELP_VIRTUAL_FUNC_PTR`,
  and the conversion it reached was 697 bytes.
- **Measured** against the commit before, 100,000 rows of four ENUM columns, on the performance cores, one run
  each. Five names written by name, 200 names by name and BIGINT were the controls; they stayed between 0.96 and
  1.04, except BIGINT on .NET 8 at 1.13 (standard deviation 0.17), so the .NET 8 figure is an indication:

  | | .NET 10 | .NET 8 |
  |---|---|---|
  | A C# enum | 0.35 (14.0 to 4.9 ms) | 0.40 (15.8 to 6.3 ms) |
  | Allocated | 18,780 KB to 30 KB | 18,780 KB to 30 KB |

  A C# enum now costs about what a BIGINT costs, and less than the same value written by its name.
- **Kept:** an enum written to a column that is not an ENUM is refused with a message that names `System.Enum`,
  not the type of the enum, as before, when the value reached the writer boxed.
- **Pinned by:** `EnumWriteConversionTests` (every integer an enum can be based on, the numbers that are refused,
  the enum as itself and as a `System.Enum`, the message for another column), which passes on the build before
  and on the build after.

### 16. A typed function writes a nullable result as the type under it

- **What:** the loops of the typed scalar functions (`DuckDBConnection.ScalarFunction.Extensions.cs`) write a
  nullable result through `WriteResult<TResult>`: one line per nullable type, `(int?)(object?)result`, then
  `WriteValue<int>` of the value or `WriteNull`. Any other result goes straight to `WriteValue`, as before. The 21
  lines are the value types a function can return, the value types of `TypeExtensions.ClrToDuckDBTypeMap` and the
  same list as `WriteNullable` in `VectorDataWriterBase`.
- **Why, found in the compiled code and measured with a micro-benchmark of the writer** (.NET 10, x64):
  - `WriteValue<int?>` passes the value by reference to `WriteNullable<int?>`, which is not inlined: about 1.2 ns per
    value (`WriteValue<int>` 1.5 ns, `WriteValue<int?>` with the value loaded from an array 2.8 ns).
  - When the JIT inlines the user's lambda, it builds the `int?` on the stack with a 1-byte and a 4-byte store and
    reads it back as one 8-byte value to pass it to `WriteValue<int?>`. A load that spans two stores still in flight
    cannot be forwarded from them, and waits: the same `int?` returned by an inlined delegate cost 6.0 ns against
    2.7 ns when it was built in a register.
- **How the value is taken out.** `ref Unsafe.As<TResult, int?>(ref result)` takes the address of the result, and the
  JIT then copies it with the same 8-byte load: the stall stays (read in the listing). The cast through `object?` is
  folded by the JIT, boxes nothing, and leaves the two fields to be read one at a time.
- **Why the call sites test the type first.** The .NET 8 JIT does not inline `WriteResult` into the loops, nor
  `ReadValue` and the user's lambda. A version that sent every result through `WriteResult` made an `int` function
  1.17 and 1.20 there, for a call that only passed the value on. With the test, which the JIT folds, an `int` result
  goes to `WriteValue` directly, and the loop is the same as before (156 bytes, read in the listing).
- **Measured** against the commit before, a million rows, on the performance cores, with an `int` and a `double`
  function as controls (0.97 and 0.99 on .NET 10; 0.94 and 0.88 on .NET 8):

  | Function | .NET 10 | .NET 8 |
  |---|---|---|
  | `int` -> `int?` | 0.46 (9.3 to 4.3 ms) | 1.06 |
  | `int?` -> `int?`, one NULL in ten | 0.63 (11.8 to 7.4 ms) | 1.02 |

  On .NET 10 a nullable result now costs what an `int` result costs. On .NET 8 there is no gain: the lambda is not
  inlined there, the result comes back in a register, and there is no stall to remove.
- **Pinned by:** `ScalarFunctionNullableTypesTests`: a function that returns its argument for all 21 types, with
  values and NULLs, compared by DuckDB with the column; and a varargs function with a nullable result. Both pass on
  the build before and on the build after.
- **Not changed:** a nullable argument is still read through the delegate of reader decision 6. Measured on its own
  it costs about 1 ns (`int?` -> `int` 4.8 against 3.8 ms for `int` -> `int`, .NET 10); `performance-open-items.md`,
  item 20.

## Costs that are known and not yet removed

- **A callback written against the interfaces pays the lookups of decision 8:** 11.0 ms where the typed form of
  the same function takes 3.6 ms. Non-generic methods on the interfaces would remove them.
- **A typed function reads a nullable argument through a delegate and tests it for NULL twice:** about 1 ns per
  argument on .NET 10. Most of the gap this line used to give (13.9 against 6.2 ms) was the nullable result, which
  decision 16 removed.
- **A list that is not a `List<T>` or an array is walked through its enumerator:** one allocated for every list,
  and two interface calls per item (decision 12).
- **A collection is tested against about 45 interfaces, for every list, to find its item type.** The types of
  text, dates and times are at the end. Trying the types of the column first halved the time of a list of three
  timestamps on .NET 8 and did nothing clear on .NET 10; it is on the branch `list-column-first`, not merged.
- **The appender creates its writers again for every chunk.** Keeping them took an ENUM with 200 names written
  by name to 0.62 of the time and changed nothing else; it is on the branch `keep-writers`, not merged, because
  a list writer then depends on what DuckDB does to a list vector when it resets a chunk.
- **A mapped property costs two indirect calls,** to its mapping and to its getter (decision 13).
- **A C# enum that reaches `WriteValue<T>` is still boxed:** a `System.Enum`, an `object`, or a nullable enum
  (decision 15). Only the appender row has an enum type in hand, and it does not go that way.
- **A row of a table function costs a delegate call,** from the loop of the scan to the method that writes the
  row, and a column a call to `WriteValue<T>` (decision 14). A source that is not an array or a `List<T>` also
  costs two interface calls on its enumerator. Compiling the loop of a chunk for a scan would remove the
  delegate call; it was not built.
- **A table function with a method that takes an `object`** boxes a row that is a struct and gives a column
  that the query does not read a writer that does nothing (decision 14).
- **The writers of a table function and of a scalar function are created for every chunk.**
- **A `DateOnly` or `TimeOnly` appended to a row** goes through the DuckDB struct (decision 6). The mapped
  appender does not (decision 13).
- **Every chained `AppendValue` is a call through `IDuckDBAppenderRow`,** because the fluent methods return the
  interface.
- **`decimal.GetBits` needs four integers on the stack,** which is the 16 bytes cleared for every decimal and a
  check of the stack guard. There is no public way to read the parts of a decimal without a buffer.
- **A decimal whose integer does not fit 64 bits, or whose scale is 20 or more away from the column's, is divided
  or multiplied on 128 bits,** and the division is a call into the runtime: 0.81 of the old time where a shorter
  value takes 0.49.

## Behaviour that is kept on purpose

- **The appender does no type checking.** It writes the bytes of the .NET type it is given. `AppendValue(1)`
  into an INTEGER column stores garbage, and a `DateTime` appended to a TIME column stores the microseconds of
  the timestamp. The mapped appender checks types when it is created. A value of the right type that is out of
  range is another matter: a decimal is checked against the width of its column (decision 10).
- **A decimal loses the digits after the point that its column has no room for, toward zero,** where DuckDB
  rounds a parameter (decision 10).
- **A TIMESTAMP_S and a TIMESTAMP_MS round toward zero before 1970,** so 1969-12-31 23:59:59.9999999 is stored
  as 1970-01-01 00:00:00. DuckDB's own cast rounds down.
- **A `DateTime` outside 1677 to 2262 wraps around in a TIMESTAMP_NS,** with no error.
- **A string ends at its first NUL character** (decision 11).
- **The expression of a column of a table function is not evaluated when the query does not read the column**
  (decision 14). A table function registered with a method that takes an `object` is still called for every
  row, and decides itself what to write.
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

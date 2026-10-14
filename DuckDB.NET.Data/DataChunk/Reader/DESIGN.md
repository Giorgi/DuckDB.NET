# Design of the vector readers

This file records why the readers in this folder are written the way they are. Each decision says what was done,
why, what was measured, and what was tried and rejected. Read it before changing the read path: several things here
look as if they could be simpler, and the simpler form was measured and lost.

The numbers are a snapshot. They were measured on one machine in October 2026 with BenchmarkDotNet, each build in
its own process, against the previous build as the baseline. A ratio below 1 means faster. Measure again before
relying on them; the ratios should hold, the absolute times will not.

## How a typed read works

`DuckDBDataReader` holds one reader per column in a `VectorDataReaderBase[]`. `reader.GetInt64(0)` goes:

```
DuckDBDataReader.GetInt64(ordinal)
  -> GetFieldValue<long>(ordinal)
    -> VectorDataReaderBase.GetValueStrict<long>(offset)      NULL check
      -> ReadValidValue<long>(offset)
           1. the column holds exactly a long     -> load it from memory          (the fast path)
           2. long is one of the fifteen types    -> ReadInt64(offset)            (a plain virtual call)
           3. anything else                       -> GetValidValue<long>(offset)  (a generic virtual call)
```

Everything from `GetInt64` down to `ReadValidValue` is inlined into the caller. The whole design is about keeping a
read out of case 3, because of the cost described next.

## The cost everything here avoids: generic virtual methods

`GetValidValue<T>` is a virtual method with its own type parameter. A method table has one slot per method, not one
per type argument, so the runtime cannot find the right override through a slot. It looks it up in a hash table on
every call (`CORINFO_HELP_VIRTUAL_FUNC_PTR` in the compiled code). The JIT cannot inline such a call, and it does
not use profile data to guess the target as it does for a plain virtual call.

- Measured in isolation: a plain virtual call 0.6 to 1.5 ns, a generic virtual call 5 to 9 ns.
- The same cost applies to a generic method called through an interface.
- A call on an object whose exact class the JIT knows is bound directly. That is why every reader class, and
  `DuckDBDataReader` itself, is `sealed`.

## Decisions

### 1. `DuckDBDataReader` is sealed

- **What:** `public sealed class DuckDBDataReader`.
- **Why:** `GetInt32` calls `GetFieldValue<int>`, which overrides a generic virtual method of `DbDataReader`. On an
  unsealed class even that call on `this` was a lookup.
- **Measured:** 5 to 9 ns less per typed getter.
- **Keep in mind:** a call through a `DbDataReader` variable still pays the lookup. Nothing here can change that.

### 2. The fast path: `StorageType`

- **What:** a reader whose vector holds plain values of one .NET type sets `StorageType` once. `ReadValidValue<T>`
  compares `typeof(T)` with it and, on a match, loads the value from memory without calling the reader.
- **Why:** the commonest read asks for the type the column has. It needs no conversion, so it needs no reader.
- **Why it is written as a type comparison:** the base class asks only "is it the same type?" and names no type. The
  subclass decides what it stores.
- **Measured:** `GetInt32` on an INTEGER column went from about 13 ns to about 1 ns per value over decisions 1 and 2.
- **Which readers set it:** the numeric reader, the boolean reader, and the BigInteger reader for HUGEINT and
  UHUGEINT (as the DuckDB structs). The string, date-time, decimal and Guid readers cannot: their values need a
  conversion.

### 3. One numeric reader class per stored type

- **What:** `NumericVectorDataReader<TStorage>`, created by the factory with the stored type fixed. HUGEINT,
  UHUGEINT and VARINT are in `BigIntegerVectorDataReader`, because their .NET type is not a plain value.
- **Why:** the earlier single class asked which of ten column types it had, in a `switch`, for every value. With the
  stored type as a type parameter it is a constant for the JIT.
- **How conversions work:** `NumericConverter.Convert` uses `CreateChecked` from generic math, so there is no table
  of type pairs. For a pair that cannot overflow, the .NET 10 JIT removes the `try`/`catch` by itself: INTEGER read as
  `long` is 15 bytes of code. .NET 8 keeps the `try`.
- **Measured:** `GetValue` (the path Dapper uses) 0.69 to 0.77; an integer column read as `double` 0.14 to 0.18, and
  it stopped allocating (it had boxed twice per value through `Convert.ChangeType`).
- **The price:** the JIT compiles one body per stored type. The first row of a result with all ten numeric types
  takes about 0.45 ms longer through `GetValue`, and opening it about 0.3 ms longer, once per process.
- **Left alone on purpose:** a FLOAT or DOUBLE read as an integer still goes through `GetValue(offset, Type)` and
  `Convert.ChangeType`, which rounds half to even. See "Behaviour" below.

### 4. `CannotCast` builds the exception

- **What:** a refused read does `throw CannotCast(typeof(A), typeof(B))`. The method is marked `NoInlining`.
- **Why:** `throw new InvalidCastException($"...")` written inline puts the buffers of the interpolated string in the
  frame of the method. The frame is cleared on every call, whether or not the method throws. The compiled hot path of
  `GetValue` began by clearing 48 to 80 bytes of stack.
- **Measured:** `GetValue` on INTEGER 944 to 198 bytes of code; a checked conversion 463 to 87. Throughput did not
  change measurably. The first-row cost of decision 3 fell from about 1.0 ms to about 0.45 ms, because each of the ten
  bodies is cheaper to compile.
- **Rule:** do not build an exception message inline in a method on the read path.
- **The same cost, found again in `DecimalVectorDataReader.GetDecimal`.** It held the conversion of all four storage
  sizes. The one for a HUGEINT divides and has a `try` block, and the method built a message for an unknown storage
  size. Every call reserved 248 bytes of stack and cleared 56 of them (.NET 10; 280 and 72 on .NET 8), also for the
  three small sizes, whose conversion is a dozen instructions. Now the wide case is `GetWideDecimal` and the message
  is `InvalidStorageType()`. Moving the wide case alone left 40 bytes cleared per call, because of the message.
  `GetDecimal` went from 1,612 to 487 bytes on .NET 10 (1,831 to 552 on .NET 8) and clears nothing.
- **Here it did change throughput:** 4,000,000 `GetDecimal` calls on DECIMAL(18,4), against the build with decision
  3: 0.79 to 0.81 on .NET 10 (four runs, 55.6 to 56.1 ms against 69 to 70 ms). On .NET 8 the four runs were 0.70,
  0.79, 0.90 and 1.52: this build measured 55, 57 and 62 ms, and 100 ms in one launch that was slow throughout. Why
  .NET 8 varies between launches is not known. Decision 5 alone measured 0.97 to 0.99 and 0.93 to 0.94 on this read.
- **So: keep a rare, expensive case out of the method that holds the common, cheap ones.** A stack frame is sized
  and cleared for everything in the method.

### 5. Fifteen typed `Read...` methods

- **What:** `VectorDataReaderBase` declares one plain virtual method for each type `DbDataReader` has a getter for
  (`ReadBoolean`, `ReadByte`, `ReadInt16`, `ReadInt32`, `ReadInt64`, `ReadFloat`, `ReadDouble`, `ReadDecimal`,
  `ReadDateTime`, `ReadGuid`, `ReadString`, `ReadStream`), and three more: `ReadDateTimeOffset`, `ReadDateOnly` and
  `ReadTimeOnly`. Each defaults to `GetValidValue<T>`. `ReadValidValue<T>` has one line per type that calls it. A
  reader overrides the ones it produces.
- **The last three were added with decision 8,** when the conversion behind them had become cheap enough for the
  lookup to show. `DbDataReader` has no getter for these types; `GetFieldValue<T>` is how they are read. Measured
  on .NET 10, 4,000,000 values, without the method and with it:
  - a DATE read as `DateOnly`: 24.7 ms and 9.5 ms;
  - a TIMESTAMPTZ read as `DateTimeOffset`: 74.5 ms and 58.7 ms;
  - a TIME read as `TimeOnly`: without the method it took twice as long as a read of the same column as `DateTime`,
    which has one (89.8 against 42.5 ms, on battery); with it the two take the same (20.7 and 23.1 ms, on mains).

  The figures of each pair are from different runs. A further target type is worth a method only when a measurement
  shows the lookup, as here.
- **Why:** after decision 3 every read that missed the fast path still paid the lookup: a numeric column read as
  another numeric type, and every `GetString`, `GetDateTime`, `GetDecimal` and `GetGuid`.
- **Why a list of types in the base class:** a cheap call needs a slot, and a slot needs a declared method per
  target type. There is no way to get one without naming the types somewhere. `DbDataReader` has the same list.
- **Why an override is one line that seems to do nothing:**

  ```csharp
  internal override long ReadInt64(ulong offset) => GetValidValue<long>(offset);
  ```

  The class is sealed, so the call to its own `GetValidValue<long>` is bound directly; the compiled override is a
  single jump. No conversion code was written or moved for this decision.
- **Rule: an override never contains a conversion of its own.** It calls `GetValidValue<T>` of its own class. Then a
  value is converted by one piece of code whichever way it is reached, and the routes cannot disagree.
- **Why `Read...` and not `Get...`:** the string and decimal readers have private `GetString`, `GetStream` and
  `GetDecimal` methods with the same signature. They would hide a base method of that name instead of overriding it,
  with only a compiler warning.
- **Why each line in `ReadValidValue<T>` returns unconditionally:** for a type with a line, the generic virtual call
  at the end of the method is unreachable. The .NET 8 JIT refuses to inline a method that contains a generic virtual
  call, but only if it has to compile that call. So these lines are what makes the read inlinable on .NET 8. Do not
  join a run-time condition to a line.
- **Measured** against the build with decision 3, on .NET 10 and .NET 8:

  | Read | .NET 10 | .NET 8 |
  |---|---|---|
  | The stored type, one column type | 0.99 | 0.61 |
  | The stored type, eight column types | 0.97 | 0.83 |
  | INTEGER as `long` | 0.46 | 0.46 |
  | Eight integer types as `long` | 0.59 | 0.51 |
  | Eight integer types as `double` | 0.57 | 0.53 |
  | `GetString`, short strings | 0.80 to 0.83 | 0.78 to 0.79 |
  | `GetDateTime` | 0.88 | 0.89 |
  | `GetInt64` on HUGEINT | 0.90 | 0.85 |
  | `GetDecimal`, `GetGuid` | 0.97 to 0.99 | 0.93 to 0.94 |
  | `GetValue` | 1.00 | 1.02 |

- **Designs measured and rejected** (`GetInt64` on 8,000,000 values, one column type and eight column types):

  | Design | One type | Eight types | Why not |
  |---|---|---|---|
  | The generic virtual method | 50 to 59 ms | 44 ms | the starting point |
  | **Plain virtual methods (this decision)** | 22 ms | 24 ms | |
  | A typed reader object, cached per column and type | 30 ms | 30 ms | slower, and the most code |
  | A `switch` on the column type that loads and widens | 19 ms | 30 ms | best on one type, worst on eight; cannot do a conversion that may fail; puts the table of which type fits which in the base class |

- **Designs considered and not built:** calling the typed methods only from the named getters of `DuckDBDataReader`
  (measured the same for `GetString`, but it does not help `GetFieldValue<T>`, nullable reads, list items or
  user-defined function inputs); casting to the concrete reader in `DuckDBDataReader` (same limit); the same casts in
  `ReadValidValue<T>` (the base class would name its subclasses, and .NET 8 would not inline).
- **A search for a design with the same speed and no typed methods in the base class** found none. Tried, on
  .NET 10, `GetInt64` on eight INTEGER columns, with this decision at 22 ms in the same series of runs:
  - one virtual `TryRead(offset, Type, ref byte destination)` that each reader overrides with a line per type it
    produces: 43 ms. The override holds the whole chain of type comparisons (246 bytes of code against 6), and the
    value travels through memory.
  - a table of function pointers per reader class, indexed by a slot per target type: 34 ms, and level with this
    decision on eight column types. A call through a function pointer gets no guess from profile data; a plain
    virtual call does.
  - the same table filled through reflection on first use, so that no class declares anything: written, not built.
    It finds methods by name at run time and uses `MakeGenericMethod`, which cannot be declared safe for trimmed and
    ahead-of-time compiled applications. This decision is safe there by construction: the compiler sees every call.
  - not built, with the reason: a generic interface per type tested with `is` (a helper call per read); a generic
    intermediate class or a generic static stub (code generic over a reference type is shared, so the call is still
    a generic virtual call); fewer, wider virtual methods (changes behaviour at the limits).
- **Getters that brought no measured speed:** `ReadDecimal` and `ReadGuid`. A `GetDecimal` cost about 16 ns and a
  `GetGuid` about 22 ns in all when this was measured (.NET 10), and the lookup hardly showed next to that. They are
  kept so that those types take the same route as the others. Both reads have been made cheaper since, in other
  ways: see decision 4 and "Smaller decisions".

### 6. Nullable reads go through `NullableReader<TValue>`

- **What:** `GetFieldValue<int?>` cannot call `ReadValidValue<int>` directly, because it only has `int?` as its type
  parameter. `NullableHandler<T>` makes a delegate once per type. The delegate is bound to an instance of
  `NullableReader<TValue>` and calls its `Read` method.
- **Why not an expression tree, as it was:** a delegate compiled from an expression tree is compiled once, with no
  profile data. After decision 5 the JIT laid its code out with the fast path jumping away and back, and the read of
  the stored type got 7% slower on .NET 10.
- **Why an instance method and not a static one:** a delegate for a static method measured slower to call.
- **How the reader for a type is found, and why it is done in three ways.** A trimmed application keeps only code it
  can see being used, and an application compiled ahead of time (Native AOT) cannot make code at run time. Finding
  `NullableReader<TValue>` by reflection alone broke both: every nullable read threw `TypeInitializationException`.
  1. **The common types and the structs of this library are named in code** (`Known<int>() ?? Known<long>() ...`).
     A compiler sees `new NullableReader<int>()` there, so the code exists in an application compiled ahead of time.
     The chain runs once per type, when its handler is set up.
  2. **Any other type, such as an enum of the caller, is found through reflection.** The `DynamicDependency`
     attribute keeps the trimmer from removing the constructor and the `Read` method, which nothing calls by name.
     This runs only where `RuntimeFeature.IsDynamicCodeSupported` is true.
  3. **Where no code can be made at run time, such a type is read as an object and unboxed:**
     `(T)reader.GetValue(offset, type)`. That needs no code for the type, so it works for every type. It boxes, and it
     converts by the rules of the `object` path, which is why the types where the two paths differ are in the list.
- **Checked with a program published both ways** (22 reads of one row): trimmed, the output is the same as with the
  expression tree. As Native AOT, `DateTime?`, `Guid?`, `bool?`, `short?` and `DuckDBDecimal?` read, where the
  expression tree threw for each because the code it called by reflection had not been compiled; and with a type
  taken out of the list it reads through the fallback. The analyzer's AOT warning on this code is gone; two trim
  warnings stay on the reflection lines, where the attribute does the work the analyzer cannot see.
- **A new type in the list costs one `?? Known<X>()`.** A type that is missing still works everywhere; under Native
  AOT it takes the slower fallback. Nothing tests the published forms automatically.
- **Measured**, ratio to the build with decision 3:

  | Read | Runtime | Expression tree | Static method | Instance method |
  |---|---|---|---|---|
  | `int?` from INTEGER | .NET 10 | 1.07 | 1.18 | 0.80 to 0.84 |
  | `int?` from INTEGER | .NET 8 | 0.97 | 0.96 | 0.31 to 0.35 |
  | `long?` from INTEGER | .NET 10 | 0.69 | 0.57 | 0.35 to 0.38 |
  | `long?` from INTEGER | .NET 8 | 0.59 | 0.53 | 0.35 to 0.37 |

  The last column is three runs: two of the form that finds the reader by reflection only, and one with the list of
  known types. The committed form, which adds the check for dynamic code and the fallback, was not measured again.
  Before this change a nullable read cost more than twice as much on .NET 8 as on
  .NET 10 (65 ms against 28 ms for 8,000,000 values); now both take about 23 ms. The compiled code shows that the
  old delegate called the read method on .NET 8 where the new method has it inlined, but one call does not account
  for the whole difference. The rest is not explained.

### 7. HUGEINT and UHUGEINT are converted through `Int128` and `UInt128`

- **What:** `BigIntegerVectorDataReader` reads an integer type from a HUGEINT by building an `Int128` from the two
  stored halves (a `UInt128` for UHUGEINT) and converting it with `NumericConverter.Convert`, the same converter
  the other integer columns use. A `BigInteger` is made only when a `BigInteger` is asked for.
- **Why:** the read used to go through `BigInteger` for every target type: `Upper * 2^64 + Lower`, which is two
  conversions to `BigInteger`, a multiplication and an addition, and then a checked conversion of the result back to
  64 bits. That is what `GetInt64` on the `sum()` of an integer column costs.
- **Why `Int128` and not a check written by hand:** a HUGEINT is exactly an `Int128`, and `Int128` takes part in
  generic math. So `CreateChecked` gives the right range check for every integer target, signed and unsigned, with no
  code of our own to get wrong at the limits. On .NET 10 the read as `long` compiles to two loads, a shift and a
  compare: the upper half must be the sign extension of the lower half. On .NET 8 the conversion is a call.
- **Why the class is still separate, and not `NumericVectorDataReader<Int128>`:** the .NET type of a HUGEINT column
  is `BigInteger` and its provider-specific type is `DuckDBHugeInt`. The generic numeric reader returns its stored
  type from `GetValue` and uses it for the fast path, so it would hand out boxed `Int128` values.
- **The message of a refused read names the column type:** "Cannot cast from HugeInt to Int64 in column x", and
  `UnsignedHugeInt` and `VarInt` likewise. These are the names `GetDataTypeName` returns. It said `BigInteger`
  before, which was the type the reader happened to convert from.
- **A `BigInteger` is made in one place:** `DuckDBHugeInt.ToBigInteger()` and `DuckDBUHugeInt.ToBigInteger()`, which
  convert from `Int128` and `UInt128` directly. They used to compute `Upper * 2^64 + Lower`. The reader calls them
  when a `BigInteger` is asked for (`GetFieldValue<BigInteger>`, `GetValue`), and so does the decimal reader when a
  DECIMAL stored as a HUGEINT is read as `BigInteger`. The two methods are public, so their results must not change.
- **The constructors go through `Int128` and `UInt128` as well** (decision 7 in `../Writer/DESIGN.md`). So the
  constructor and `ToBigInteger()` do not check each other: a round trip would not notice a mistake they share.
  `HugeIntToBigIntegerGivesBackTheValue` and `UHugeIntToBigIntegerGivesBackTheValue` compare the two halves with
  the ones division gives, and then convert back.
- **A wide DECIMAL (more than 18 digits, stored as a HUGEINT) read as `decimal` is divided as an `Int128`:**
  `Int128.DivRem` by a power of ten from `DecimalExtensions.Int128PowersOfTen`, then the same conversion of quotient
  and remainder as before. It used `BigInteger.DivRem`, which allocates for the value, the quotient and the
  remainder. The steps were kept on purpose, so the result has the same scale as before: `1.5` from a DECIMAL(38,4),
  where a DECIMAL(18,4) gives `1.5000`. The old and the new conversion were compared on the bits of the result and
  on whether they throw, for every scale from 0 to 38: 46,820,592 conversions on each runtime, none different.
- **Not changed:** `GetValue` and `GetFieldValue<BigInteger>` return a `BigInteger`; a read as `float`, `double` or
  `decimal` is refused; VARINT is stored as a byte string and still goes through `BigInteger`.
- **Measured** against the build with decision 3, 1,000,000 rows of four HUGEINT columns read with `GetInt64`, two
  runs on each runtime: 146 and 150 ms to 30 ms on .NET 10 (0.20 to 0.21), 151 and 154 ms to 33 ms on .NET 8 (0.22).
  The build with decision 5 measured 134 and 130 ms in an earlier, separate run. Per value, moving through the rows
  included: about 37 ns before, 7.5 ns after on .NET 10 and 8.3 ns on .NET 8.
- **Measured, the reads that make a `BigInteger` and the wide DECIMAL,** ratio to the build with decision 3,
  4,000,000 values each. The machine was not quiet for any of these runs (standard deviation up to 10% of the
  mean), so the .NET 10 column is the range over two runs, three for the wide DECIMAL, and .NET 8 is one run:

  | Read | .NET 10 | .NET 8 | Allocated |
  |---|---|---|---|
  | HUGEINT with `GetValue` | 0.38 to 0.44 | 0.42 | 125 MB before and after (the box) |
  | HUGEINT as `BigInteger`, values that fit 32 bits | 0.43 to 0.50 | 0.39 | none before or after |
  | HUGEINT as `BigInteger`, larger values | 0.33 to 0.36 | 0.31 | 437 MB to 156 MB |
  | DECIMAL(38,x) with `GetDecimal` | 0.46 to 0.69 | 0.69 | 107 MB to none |

  The wide DECIMAL took 207 to 265 ms on .NET 10 where the earlier build took 381 to 505 ms, and 278 ms against
  407 ms on .NET 8: 52 to 70 ns per value, moving through the rows included.
- **Pinned by:** `HugeIntAtTheLimitsOfLongAndULong`, ten values just inside and just outside what `long` and `ulong`
  hold, for both column types; and `OverflowingHugeIntReadNamesTheColumnTypeAndTheColumn`.

### 8. A DATE, a TIME and a timestamp are converted by arithmetic

- **What:** `DateTimeVectorDataReader` converts a DATE read as `DateTime` or `DateOnly`, and a TIMESTAMP,
  TIMESTAMP_S, _MS, _NS or TIMESTAMPTZ read as `DateTime` or `DateTimeOffset`, itself. DuckDB stores a DATE as days
  and a timestamp as a count in the unit of the column, both from 1970-01-01. A `DateTime` counts ticks of 100 ns
  from 0001-01-01. So the read is one multiplication, one addition and one comparison for the range. Both paths do
  it: the typed one and `GetValue(offset, Type)`.
- **TIME and TIMETZ the same way.** A TIME is microseconds from midnight, so as `TimeOnly` or `DateTime` it is
  `micros * 10` ticks. A TIMETZ is 64 bits: the microseconds in the upper 40, and in the lower 24 the offset from
  UTC, stored as 57,599 (the largest offset, 15:59:59) minus the offset in seconds. That layout is `dtime_tz_t` in
  DuckDB's `datetime.hpp`, read at `v1.5.6`; the constants carry the reference. As `DateTimeOffset`
  the time is put on 0001-01-02 (see "Behaviour"). DuckDB used to divide each value into hour, minute, second and
  microsecond (`duckdb_from_time`, `duckdb_from_time_tz`), and the reader multiplied them back.
- **Why:** the read used to ask DuckDB whether the value was finite (`duckdb_is_finite_*`), have DuckDB split it into
  year, month, day and time (`duckdb_from_timestamp`, `duckdb_from_date`), and put the parts back together with
  `new DateTime(year, month, day).AddTicks(...)`. The split measured 18 of the 22.9 ns of a timestamp conversion.
- **Infinity is two stored values,** the same two for every timestamp unit and two for a DATE, and the bindings have
  them as constants (`IsInfinity`). DuckDB's own functions make that comparison. An infinite value read as a .NET
  type throws `InvalidOperationException` with the message it always had.
- **The range check is one unsigned comparison** per read (`(ulong)(units - minimum) > (ulong)(maximum - minimum)`).
  It also catches the infinity markers, so the common path has no separate test for them; the helper that builds
  the exception tells the two apart. TIMESTAMP_NS has no range check: its whole range, 1677 to 2262, is inside a
  `DateTime`, so there the two markers are tested directly.
- **TIMESTAMP_NS: `ticks = nanoseconds / 100`,** dropped toward zero for both signs. The earlier code did the same,
  and so does DuckDB when it casts a TIMESTAMP_NS to a TIMESTAMP.
- **A value outside the .NET type** throws `ArgumentOutOfRangeException`, as before, with a message that names the
  column and the DuckDB type that can hold it. Readers of function arguments have no column name and leave it out.
- **The write side does the same arithmetic in the other direction** (decision 6 in `../Writer/DESIGN.md`). The
  epoch and the TIMETZ constants are in `Extensions/DateTimeExtensions.cs`, one copy for both directions, and the
  reader refers to them. A change to a constant changes reads and writes together, so a round trip does not show
  it; the guard below and the one of the write side do.
- **What still calls DuckDB:** a read as one of the DuckDB struct types, `DuckDBDateOnly`, `DuckDBTimestamp`,
  `DuckDBTimeOnly` or `DuckDBTimeTz`, which needs the parts. No read as a .NET type does.
- **A defect that went with it:** `make_timestamp` can store a finite TIMESTAMP below DuckDB's own minimum
  (duckdb/duckdb#26797). `duckdb_from_timestamp` throws a C++ exception for it, which reached the caller as
  `SEHException`. Catching that is not safe: in a review the process died within 20,000 such reads, on both
  runtimes. A read as `DateTime` no longer goes there. A read as `DuckDBTimestamp` still does; that is left to
  DuckDB, and the message for such a value does not recommend it.
- **The guard, because this code now assumes how DuckDB stores these types:**
  `DuckDB.NET.Test/TemporalConversionTests.cs` compares each read with what DuckDB itself says about the same value:
  the split (`GetFieldValue<DuckDBTimestamp>`), the text (`strftime`) and the count (`epoch_us`). Every day a
  `DateTime` holds is in it, each timestamp type, every second of the day for a TIME (against DuckDB's `hour`,
  `minute` and `microsecond`), and 49 pairs of time and offset for a TIMETZ. If a DuckDB release changes a unit, an
  epoch, a marker or the bits of a TIMETZ, it fails. Do not replace those comparisons with constants worked out in
  C#.
- **Checked:** the old and the new build on about 430,000 result lines per runtime (stored values through every
  accessor, lists, structs, maps, nullable targets, function arguments, three session time zones), nothing different
  but the exceptions above; 40 deliberate mistakes in the new code, 39 caught by the guard class, the last one in
  code SQL cannot reach. That was for dates and timestamps. For TIME and TIMETZ: a probe of 21 values through six
  accessors, exception messages included, printed the same before and after on both runtimes; no second reviewer
  and no deliberate mistakes there.
- **Measured** against `1.5.6-alpha.53`, 4,000,000 values, ratio of the time. .NET 8 is one run; on .NET 10 the two
  rows with a typed method of decision 5 are from a second run, and the TIME and TIMETZ rows from a third:

  | Read | .NET 10 | .NET 8 |
  |---|---|---|
  | TIMESTAMP as `DateTime` | 0.15 (125.9 to 19.3 ms) | 0.20 (117.0 to 23.0 ms) |
  | TIMESTAMP_NS as `DateTime` | 0.17 | 0.18 |
  | TIMESTAMPTZ as `DateTimeOffset` | 0.37 | 0.40 |
  | TIMESTAMP with `GetValue` | 0.24 | 0.28 |
  | DATE as `DateTime` | 0.16 (68.4 to 10.6 ms) | 0.24 (63.6 to 14.9 ms) |
  | DATE as `DateOnly` | 0.12 | 0.19 |
  | TIME as `TimeOnly` | 0.21 (100.9 to 20.7 ms) | 0.21 (126.3 to 26.6 ms) |
  | TIME as `DateTime` | 0.27 | 0.29 |
  | TIMETZ as `DateTimeOffset` | 0.45 | 0.43 |
  | The four TIME columns with no value read (control) | 1.04 (14.2 and 14.7 ms) | 1.03 |

  The test for infinity alone, before the arithmetic, measured 0.65 and 0.85 on the two DATE reads and nothing
  measurable on timestamps (.NET 10).

### 9. The list, struct and map readers look at the target type once per column

- **What:** each of the three readers keeps what it worked out about the type it was asked for last, and works it
  out again only when the type changes. A column is read as the same type row after row.
  - **Lists** (`ListVectorDataReader`): the element type, whether an item can be NULL, and which loop to take
    (`CreatePlan`). `List<int>`, `List<float>`, `List<double>`, `List<decimal>` and their nullable forms are created
    as `new List<T>(length)` and filled by `BuildList<T>` or `BuildNullableList<T>`, which test an item for NULL once
    and read it with `ReadValidValue<T>`. It was tested twice, three times for a nullable item, and the list was made
    by `Activator.CreateInstance` and grew. Any other type, and a class derived from a list type, is created as
    before. `ReadValidValue<T>` is internal for this.
  - **Structs** (`StructVectorDataReader`): the properties of the target type, each paired with the reader of its
    field (`PairPropertiesWithReaders`). It was a case-insensitive dictionary lookup for every property of every row,
    and a lookup in the process-wide cache of types. The lambda for that cache is static: one that used the
    parameter of the method made C# allocate a closure at the start of the method, for every row, also on the path
    for `Dictionary<string, object>`, which never reaches the lambda; on .NET 8 a delegate as well.
  - **Maps** (`MapVectorDataReader`): the types a key and a value are read as, and whether a value can be NULL
    (`CreatePlan`). `GetGenericArguments` allocated an array for every row.
- **`CreatePlan` of the list reader is `NoInlining`.** Inlined, its reflection made `GetList` set up a frame for a
  native call and clear a 184-byte frame on every row (Tier 1, both runtimes): 2,749 bytes of code against 965. An
  A/B of the attribute alone measured no difference, 1.01 and 0.94 (.NET 10). Kept because it is one line and the
  work it removes is in the listing.
- **Measured** against the commit before, 200,000 rows, on the performance cores; .NET 10 in two runs, .NET 8 in
  one. The control is an INTEGER column read as `int` (1.00 to 1.03):

  | Read | .NET 10 | .NET 8 | Allocated per row |
  |---|---|---|---|
  | `INTEGER[5]` as `List<int>` | 0.30, 0.33 | 0.38 | 160 to 80 B |
  | `INTEGER[]` with NULL items as `List<int?>` | 0.30 | 0.32 | 240 to 96 B |
  | `INTEGER[]` of 0 to 9 items as `List<int>` | 0.41 | 0.43 | 137 to 74 B |
  | `INTEGER[5]` as `List<long>` | 0.69, 0.67 | 0.77 | 448 to 416 B |
  | STRUCT as a class | 0.63, 0.64 | 0.62 | 148 to 124 B; 207 to 124 B on .NET 8 |
  | STRUCT as `Dictionary<string, object>` | 0.97, 0.95 | 1.05 | 324 to 300 B |
  | MAP as `Dictionary<int, string>` | 0.53, 0.57 | 0.68 | 316 to 276 B |

- **Nothing that is read changed.** `CompositeReadTargetTypeTests` reads one column as different types in turn,
  and as a class derived from a list type; it passes on the build before and on the build after.
- **Left for 2.0** (`issues/deferred-to-2.0.md`, item 4): every item of a list of another type, every key and value
  of a map and every field of a struct is still read as an object and boxed, and such a list is still made by
  `Activator` and grows. Reading them on the typed path changes what some reads return, which is why it waits for
  the shared conversion.

### Smaller decisions

- **`BigIntegerVectorDataReader` is a separate, non-generic class.** HUGEINT, UHUGEINT and VARINT read as
  `BigInteger`, which is not a plain value, so they cannot be `NumericVectorDataReader<TStorage>`. Loosening the
  constraint on `TStorage` to fit them in would make the class compile to code shared between types, and the stored
  type would stop being a constant for the JIT.
- **`DecimalVectorDataReader` has no inner reader.** It reads a HUGEINT-backed decimal from its own data pointer. It
  used to hold a numeric reader only for that, and had to reset and dispose it with every chunk.
- **A UUID is converted to a `Guid` from its parts** (`GuidConverter`, in `Extensions`). DuckDB stores a UUID as the
  128-bit number its text form spells, with the top bit flipped, so the two halves hold exactly the parts the `Guid`
  constructor takes. The conversion used to write both halves into a 32-byte stack buffer, move 16 bytes one at a
  time through a table of positions and build the `Guid` from a span: 188 bytes of code with a loop, against 116
  with none (.NET 10). The write direction reads two big-endian 64-bit numbers from the bytes of the `Guid`. Old and
  new were compared in both directions on 40,001,412 values per runtime, none different. Measured, 4,000,000
  `GetGuid` calls against the build with decision 3: 95.9 to 66.1 ms on .NET 10 (one run, 0.69); on .NET 8 two
  noisy runs, 0.44 and 0.55. The write direction is not measured.
- **A column type with no reader gets a plain `VectorDataReaderBase`** from the factory (a UNION column, for
  example). The query runs, and every read of a value throws "Unrecognised type". Do not make a real reader the
  default: it would return values read with the wrong layout.
- **The boolean reader returns two shared boxed objects from `GetValue`.** `GetValue` returns `object`, so a value is
  boxed on every call; a boolean has two values, so the boxes are made once. Reading a BOOLEAN column through
  `GetValue` allocates nothing. No other type can do this.
- **Strings are decoded with `Encoding.UTF8.GetString(pointer, length)`,** not with the `string` constructor that
  takes an encoding. Both end in the same decoder; the constructor validates more on the way. Short strings measured
  7 to 8% faster.
- **`GetValue(offset, Type)` in the numeric reader compares the requested type with `typeof(TStorage)` first** and
  returns the box it already has. `Convert.ChangeType` does not return early for the same type; it boxes again.

### Costs that are known and not yet removed

- **`DateTimeVectorDataReader.GetValidValue<T>` clears part of its stack frame on every call:** 40 bytes for
  `TimeOnly` and `DateOnly`, 24 for `DateTime` (.NET 10). The method holds the branches of every column type it
  serves, and the ones that return a DuckDB struct need stack slots (decision 4). One method per column type would
  remove it and might add a call in its place. Not measured; a TIME value costs about 2 ns in all as it is.
- **A TIMESTAMPTZ or TIMETZ read as `DateTimeOffset` keeps about 15 ns per value** where a `DateTime` takes 5: the
  constructor of `DateTimeOffset` is a call that validates the offset. That is the cost of the type.
- **A read as `DuckDBTimestamp`, `DuckDBDateOnly`, `DuckDBTimeOnly` or `DuckDBTimeTz` calls DuckDB,** by design
  (decision 8).
- **`TIME_NS` has no reader.** A column of that type gets the plain `VectorDataReaderBase` and every read throws
  "Unrecognised type 39".
- **A wide DECIMAL (stored as a HUGEINT) is still divided on every read:** about 50 to 70 ns per value, where a
  DECIMAL(18,4), which is built from the stored integer and the scale with no division, takes about 14 ns. A value
  that fits 96 bits with a scale of at most 28 could be built the same way. That is not done because it changes what
  the value looks like: `1.5000` where it is `1.5` today. Its speed is not measured.
- **`GetValue` boxes every value** except booleans. It returns `object`; nothing can be done about it here.
- **A scalar function whose callback is written against the interfaces reads its inputs through
  `IDuckDBDataReader.GetValue<T>`,** a generic method on an interface, which is looked up at run time on every
  value. Decision 5 removed the second lookup behind it, not this one. The typed forms
  (`RegisterScalarFunction<int, int>("f", x => x + 1)`) do not: their loops call `GetValue<T>` on
  `VectorDataReaderBase`, where it is not virtual and is inlined (decision 8 in `../Writer/DESIGN.md`).

## Behaviour: what converts to what

`DuckDB.NET.Test/NumericReaderConversionTests.cs` pins the numeric part of this, `DuckDBDataReaderEnumTests.cs` the
ENUM row, and `TemporalConversionTests.cs` and `Parameters/TimeTests.cs` the dates, times and timestamps at the end.

| Column | As an integer type | As `float` or `double` | As `decimal` | As `BigInteger` |
|---|---|---|---|---|
| TINYINT to UBIGINT | the value if it fits | the value | the value | the value |
| FLOAT, DOUBLE | the value, rounded half to even, if it fits | the value | the value | refused |
| HUGEINT, UHUGEINT, VARINT | the value if it fits | refused | refused | the value |
| DECIMAL | refused | refused | the value | the value if it has no fraction |
| ENUM | its position in the list of names, if it fits | refused | refused | its position |

- A refused read throws `InvalidCastException`. Where this code refuses it (an integer that does not fit, a DOUBLE
  out of range, an ENUM), the message names both types and the column: `Cannot cast from Int32 to Byte in column x`.
  Where the runtime refuses it (HUGEINT as `double`, DOUBLE as `BigInteger`, DECIMAL as an integer), the message is
  the runtime's and has no column. A DECIMAL with a fraction read as `BigInteger` names the column but not the types.
- An ENUM column also reads as `string` (the name) and as a C# enum type of any size. The enum reader converts a
  C# enum through the size of that enum: `Unsafe.As` reads as many bytes as the target type has, so giving it a
  smaller value reads past it. It once did, for every type but `string`.
- DOUBLE as `float` is the C# cast with no check: digits are lost, and a value too large reads as infinity. Npgsql
  does the same.
- FLOAT or DOUBLE as `decimal` is the C# cast: a `double` keeps 15 significant digits and a `float` 7. NaN, infinity
  and a value out of range are refused. Npgsql refuses this read altogether; here it is allowed on purpose.
- **Two paths with different rules.** A column, and a list item of type `int`, `float`, `double` or `decimal`, are
  converted by the typed path above. A map key or value, a struct field read into a class, and every other list item
  are converted by `GetValue(offset, Type)` with `Convert.ChangeType`. The two agree for FLOAT, DOUBLE and `decimal`.
  They differ for `BigInteger`: an integer column reads as `BigInteger` on the typed path only, and a HUGEINT reads
  as `long` on the typed path only (decision 7 is on the typed path).
- **Planned for the next major version:** a value with a fraction read as an integer type will throw instead of
  rounding, for FLOAT, DOUBLE and DECIMAL alike; and both paths will share one conversion.

Dates, times and timestamps (decision 8):

- A DATE reads as `DateOnly` (what `GetValue` returns), `DateTime` at midnight, or `DuckDBDateOnly`. A timestamp of
  any unit reads as `DateTime` (what `GetValue` returns), `DateTimeOffset` with offset zero, or `DuckDBTimestamp`.
- A `DateTime` has `Kind` `Unspecified`, also for a TIMESTAMPTZ, which DuckDB stores in UTC.
- A TIMESTAMP_NS loses what is below 100 ns, toward zero. Read as `DuckDBTimestamp` it loses what is below a
  microsecond.
- Infinity reads only as the DuckDB types; as a .NET type it throws `InvalidOperationException`.
- A finite value outside the .NET type throws `ArgumentOutOfRangeException`. The two paths agree on all of this.
- A TIME reads as `TimeOnly` (what `GetValue` returns), as `TimeSpan` (the time since midnight), as `DateTime` on
  0001-01-01, or as `DuckDBTimeOnly`. `24:00:00`, which DuckDB allows, is refused as `TimeOnly`
  (`ArgumentOutOfRangeException` from its constructor), is one day as `TimeSpan`, and is 0001-01-02 as `DateTime`.
  Npgsql reads a `time` as `TimeOnly` and `TimeSpan` in the same way. A TIMETZ does not read as `TimeSpan`, there
  as here. `TimeSpan` has no typed `Read...` method (decision 5); nothing has measured that read.
- A TIMETZ reads as `DateTimeOffset` (what `GetValue` returns) with the time on 0001-01-02, or as `DuckDBTimeTz`.
  A `DateTimeOffset` needs a date. On 0001-01-01 a time earlier in the day than its own positive offset, such as
  `00:30:00+01`, would be an instant before year 1, which `DateTimeOffset` refuses; Npgsql uses the second day for
  the same reason. `24:00:00` is the start of 0001-01-03. An offset beyond 14 hours or with seconds, which DuckDB
  allows, has no `DateTimeOffset` and reads only as `DuckDBTimeTz`. Pinned in `Parameters/TimeTests.cs`.

## Rules for changing this code

1. Do not add a virtual method with its own type parameter to the read path, and do not call one through an
   interface in a loop.
2. Keep every reader class sealed.
3. A typed `Read...` override calls `GetValidValue<T>` of its own class and nothing else.
4. A line in `ReadValidValue<T>` is `if (typeof(T) == typeof(X)) return ...;` with nothing else in the condition.
5. Build exceptions in a helper, not inline.
6. A new reader class is registered in `VectorDataReaderFactory`, overrides `Reset` if it has child readers, and
   overrides the `Read...` methods for the types it produces.
7. Measure before and after, against the previous build in the same run, with a read that must not change as the
   control, on one column type and on several, and on both target frameworks. A design that wins on one column type
   can lose on several.
8. Read the compiled code (`DOTNET_JitDisasm`) for a change on the read path. Several of the facts above were visible
   only there.

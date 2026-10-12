# AGENTS.md: vector readers and writers

The classes in `Reader` are the read path of `DuckDBDataReader` and of the arguments of functions. The classes in
`Writer` are the write path of the appender and of the results of functions. One value read or written here runs
millions of times per query, and the code was shaped by measurement.

**Read the design file of the folder before changing anything in it:** `Reader/DESIGN.md` or `Writer/DESIGN.md`.
Each says why its code is written as it is and which simpler forms were measured and rejected.

**Look at both folders.** The readers and the writers mirror each other: a type that is read in one is written in
the other, often by the same arithmetic and the same constants (dates and times, HUGEINT, UUID). Before changing
how a type is read, look at how it is written, and the reverse. A change to a shared constant changes both
directions together, so a round trip does not show a mistake; the tests that compare with DuckDB do.

## Rules for both paths

- No new virtual method with its own type parameter, and no generic method called through an interface in a loop.
  Such a call is looked up at run time on every value.
- A test of a type parameter has the form `typeof(T) == typeof(X)`, or `typeof(T).IsValueType`, with no other
  condition. The JIT folds those. It does not fold a pattern test of a nullable value, which boxes it.
- An exception is built in a helper such as `CannotCast` or `ThrowColumnOutOfRange`, never with an interpolated
  string inline in a method that reads or writes.
- A conversion returns or stores what it did before, unless the change is meant to change it. Compare the build
  before with the build after on real values.

## Rules for the readers

- Every reader class stays `sealed`.
- A typed `Read...` override (`ReadInt64`, `ReadString`, ...) is one line that calls `GetValidValue<T>` of its own
  class. It never contains a conversion.
- A line in `VectorDataReaderBase.ReadValidValue<T>` has the form `if (typeof(T) == typeof(X)) return ...;`.
- A new reader class is registered in `VectorDataReaderFactory`, and overrides `Reset` if it has child readers.
- Behaviour of numeric conversions is pinned by `DuckDB.NET.Test/NumericReaderConversionTests.cs`, and of dates
  and times by `DuckDB.NET.Test/TemporalConversionTests.cs`.

## Rules for the writers

- A new writer class is registered in `VectorDataWriterFactory`.
- The code that calls the writers is part of the write path: `DuckDBAppenderRow`, the mapped appender in
  `Mapping`, and the registration of scalar and table functions. `Writer/DESIGN.md` covers it.
- What a .NET date or time is stored as is pinned by `DuckDB.NET.Test/TemporalWriteConversionTests.cs`.
- Take allocation figures from a Release build. A Debug build boxes every value once in `WriteValue<T>`.

## Before proposing a change as faster

Measure it: against the previous build in the same BenchmarkDotNet run, with a read or a write that must not
change as a control, on one column type and on several, on net8.0 and net10.0. If the control moves, the run is
not usable. State no gain and no cause that was not measured.

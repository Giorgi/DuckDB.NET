# AGENTS.md: vector readers

These classes are the read path of `DuckDBDataReader`. One value read here runs millions of times per query, and
the code was shaped by measurement. **Read `DESIGN.md` in this folder before changing anything here.** It says why
each part is written as it is and which simpler forms were measured and rejected.

Rules that must hold after any change:

- Every reader class stays `sealed`.
- No new virtual method with its own type parameter on the read path, and no generic method called through an
  interface in a loop. Such a call is looked up at run time on every value.
- A typed `Read...` override (`ReadInt64`, `ReadString`, ...) is one line that calls `GetValidValue<T>` of its own
  class. It never contains a conversion.
- A line in `VectorDataReaderBase.ReadValidValue<T>` has the form `if (typeof(T) == typeof(X)) return ...;` with no
  other condition.
- An exception on the read path is built in a helper such as `CannotCast`, never with an interpolated string inline.
- A new reader class is registered in `VectorDataReaderFactory`, and overrides `Reset` if it has child readers.

Before proposing a change as faster, measure it: against the previous build in the same BenchmarkDotNet run, with a
read that must not change as a control, on one column type and on several, on net8.0 and net10.0. State no gain and
no cause that was not measured.

Behaviour of numeric conversions is pinned by `DuckDB.NET.Test/NumericReaderConversionTests.cs`.

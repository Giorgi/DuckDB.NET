# Migrating DuckDB.NET to the DuckDB C API V2

DuckDB ships a second C API, `duckdb_v2.h`, next to the original `duckdb.h`. Both are exported by the same native
library (the v2.0.1 release exports 641 `duckdb_v2_*` symbols on every platform DuckDB.NET ships), but every V2
function is still marked `unstable`: DuckDB plans to stabilize it within the next months. This document is the plan
for moving `DuckDB.NET.Data` onto V2, one stage at a time, on the `nightly-vnext` branch.

## How V2 differs from V1

| Area | V1 | V2 |
|---|---|---|
| Opening a database | `duckdb_open_ext` with a config object | environment, then instance (startup options as text), then `instance_attach` a database, then connection. `instance_open_shared` replaces V1's instance cache. |
| Errors | `duckdb_state` and a different error getter for each object | Every function returns a `DUCKDB_V2_ERROR` code and takes an optional error info handle as its last argument. The codes are grouped (1xxx IO, 2xxx input, ...) and are not V1's numbers. |
| Strings | null-terminated; some must be freed with `duckdb_free` | pointer and length in; borrowed from their owner out. V2 has no `duckdb_free`. |
| Running SQL | `duckdb_query`, extract statements, prepare | `parse_sql`, then iterate the statements, then `statement_execute`. A syntax error is reported by `statement_iterator_next`. |
| Results | materialized or streaming | streaming only. No materialized result and no `duckdb_value_*`. |
| Open results | any number per connection | one: a second statement while a result is open fails with `RESOURCE_IN_USE` (3001). |
| Parameters | `duckdb_bind_*` per parameter, kept until cleared | an array of values passed with each execution, by position or by name. |
| Logical types | per kind getters (`duckdb_decimal_width`, ...), `DUCKDB_TYPE` numbers | generic `logical_type_get_param`; the engine's internal type ids (VARCHAR = 25, INTEGER = 13). |
| Vectors | always flat | a view `{data, validity, sel, count}` that may be constant or dictionary. The memory layouts (strings, list entries, hugeint, interval, decimal and enum storage) are the same as V1. |
| Writing vectors | `duckdb_data_chunk_set_size`, string assign helpers | `vector_set_size` on each vector; strings go into the vector's arena by hand. |
| Appender | `duckdb_appender_*` | none. A column data collection is attached to `INSERT ... SELECT * FROM buf` with `statement_add_collection`. |
| Functions | scalar functions, function sets, table functions | registered on a connection, transactionally; no function sets; errors go into the callback's `err` slot. |
| Arrow | `arrow_scan` and the chunk converters | results through `statement_execute_arrow`; the converters need a context that only callbacks have. |

V1 and V2 handles cannot be mixed: no function turns a V1 database or connection into a V2 one. A connection opened
through V2 has to do everything through V2, so `DuckDB.NET.Data` moves to V2 as a whole on this branch. V1 functions
that take none of those handles keep working next to V2, for example `duckdb_config_count`, `duckdb_get_config_flag`,
`duckdb_library_version` and the date and time conversions (`duckdb_from_date`, `duckdb_from_time_tz`, ...).

## Ground rules

- The branch builds against DuckDB's nightly libraries, which have the signatures of DuckDB's `main` branch.
- The V2 bindings are generated from `duckdb_v2.h` by ClangSharpPInvokeGenerator (pinned in `dotnet-tools.json`) with
  `scripts/generate-v2-bindings.sh`, into `DuckDB.NET.Bindings/NativeMethods/V2/Generated`. They are raw and
  blittable (`DllImport`, typed pointers, the C names) and `internal` until DuckDB marks V2 stable; Bindings gives
  Data and Test access. CI regenerates them on nightly builds and fails when they no longer match the header, so a
  change of DuckDB's API shows up as a failed check and then as compile errors at the call sites. ClangSharp emits
  `DllImport` rather than `LibraryImport`: for blittable signatures the two are the same.
- Only the long-lived handles get hand-written SafeHandles (`DuckDBV2Environment`, `DuckDBV2Instance`,
  `DuckDBV2Connection`, `DuckDBV2Result`, later the prepared statement), whose `Acquire()` lease keeps the handle
  alive during a call. Borrowed handles such as vectors stay plain pointers, as the V1 code passes vectors as
  `IntPtr`.
- The V1 declarations stay: they are public API of the Bindings package. Whether to remove them is decided at the
  end. A public V2 API in Bindings, if wanted, is written once V2 is stable.
- Each stage names the tests that must pass before the next one starts. A test that passes on V2 gets
  `[Trait("V2", "Ready")]`, and CI on `nightly-vnext` runs only those (`--filter V2=Ready`), so the run stays green
  while the rest of Data is broken. Locally: `dotnet test DuckDB.NET.Test/Test.csproj --filter V2=Ready`. When the
  migration is done, the traits and the filter go.
- Code that is not migrated yet still compiles against V1, and fails when it runs: `DuckDBConnection.NativeConnection`,
  the V1 handle every unmigrated path goes through, throws `NotSupportedException`.

## Stage 0: groundwork

No behaviour change.

- `DuckDBErrorCode`, the V2 error codes as a public enum (a test keeps it equal to the generated one), and the mapping of each code to the public `DuckDBErrorType`. The mapping is
  the inverse of the engine's `GetErrorCodeFromExceptionType` (`src/main/capi/v2/capi_v2.cpp`), so it is one to one
  for every engine exception type. The V2-only codes go to the nearest type.
- `DuckDBException.NativeErrorCode`: the exact V2 code, next to `ErrorType`.
- A `ThrowIfFailed` helper that reads the error info text, destroys the info and throws. `RUNTIME_INTERRUPT` becomes
  `OperationCanceledException`, as it does for V1 results.
- UTF-8 strings: pointer and length in, borrowed `(pointer, length)` out, never freed (`DuckDBV2Utf8`).
- The generated bindings, the generation script and the CI check.
- SafeHandles for the environment, instance, connection and result, following V2's "destroy takes `T*` and sets it to
  null" convention.

## Stage 1: open and close (done)

- One environment for the process (`Connection/DuckDBV2Startup.cs`), created on first use and never destroyed:
  `environment_destroy` refuses while an instance is alive anyway.
- `ConnectionManager` keeps its cache and reference counting, because it also covers `:memory:?cache=shared`, which
  `instance_open_shared` never shares. `duckdb_create_config`, `duckdb_set_config` and `duckdb_open_ext` became
  `instance_create_with_options` (the connection string's options, `duckdb_api` among them) followed by
  `instance_attach(path or ":memory:", make_default: true)`. `duckdb_connect` became `connection_create`, for both
  `GetConnectionReference` and `DuplicateConnectionReference`. `FileReference` holds a `DuckDBV2Instance` and
  `ConnectionReference` a `DuckDBV2Connection`.
- `ServerVersion` uses `library_version`, and `GetQueryProgress` uses `connection_progress_get`.
- `DuckDBConnection.NativeConnection` (public, a V1 handle) throws `NotSupportedException` on an open connection:
  a V2 connection has no V1 handle. Whether it goes, or what replaces it, is decided in stage 8.
- Found on the nightly library (`V2OpenTests`):
  - `access_mode=READ_ONLY` still works as a startup option; a read-only open of a missing file fails and does not
    create it.
  - `lock_configuration=true` is accepted at open, with other options.
  - A refused value fails the whole create call, and DuckDB's message does not always name the option (`threads=-1`
    gives "Must have at least 1 thread!"). V1 named the option but dropped DuckDB's reason. On failure DuckDB.NET now
    tries each option on its own and reports both: "Error setting 'threads' to '-1': Syntax Error: Must have at least
    1 thread!".
  - Opening and closing is as fast as with V1: `ConnectionSpeed` ran at 108 to 134 operations a second on V2 against
    108 to 113 on V1, with the same nightly library.
- Not checked yet, because it needs SQL: the default database's name for a file (V1 named it after the file), and
  MotherDuck (`md:`) data sources, which go through `instance_attach` now.
- Tests: `V2OpenTests`, `V2GroundworkTests`, `OpenTests`, the open, close, state and duplicate parts of
  `DuckDBConnectionTests` (`ConnectionSpeed` among them), `DuckDBClientFactoryTests` without `UseFactory`, and the
  parsing and open tests in `ConnectionStringTests`. The tests that check a setting by running SQL, and those that
  build the option descriptors from `duckdb_settings()`, move to stage 2.

## Stage 2: queries without parameters

Done in steps: first the execution path and the primitive types (step 1, done), then dates, times, decimals,
hugeint, uuid, interval and enum (step 2, done), then list, array, struct and map (step 3). UNION has no reader in DuckDB.NET, so it
keeps the base reader, as with V1.

### Decisions

- **Results only stream, as V2's do.** `UseStreamingMode` defaults to true. `UseStreamingMode = false`
  (materialized results, V1's default) throws `NotSupportedException` for now; it is not implemented on this branch.
- **One open reader per connection.** V2 refuses another statement while a result is open
  (`RESOURCE_IN_USE`). DuckDB.NET reports that as `InvalidOperationException` ("The connection has an open data
  reader..."), as SqlClient does without MARS, with DuckDB's error as the inner exception. With V1's materialized
  results, a forgotten reader did no harm; now it keeps the connection busy until it is closed or collected.

### Step 1: the execution path and the primitive types (done)

- `Execution/DuckDBV2Statements.cs` parses the text (`parse_sql`), takes one statement at a time
  (`statement_iterator_next`, which also reports syntax errors) and runs it (`statement_execute`). The next statement
  runs only when the reader moves to it, after the previous result is done.
- `Execution/DuckDBV2StatementResult.cs` wraps a result. V2 runs a statement while its result is read, and an
  undrained result applies nothing, so:
  - a statement that returns no rows (DDL, an INSERT without RETURNING) is always drained, also through
    `ExecuteReader`;
  - a result left before its end (`NextResult`, `Close`, `ExecuteScalar`) is aborted when it is a SELECT, and run to
    its end otherwise (an INSERT ... RETURNING), so what it writes is applied as with V1;
  - `ExecuteNonQuery` runs every statement to its end. `RecordsAffected` counts a result's rows when the reader
    leaves it;
  - a statement that DuckDB expands into several (a dynamic PIVOT) reports its shape only after its first chunk
    (`INPUT_METADATA_DEFERRED`): the first chunk is fetched early and kept for the reader.
- `DuckDBDataReader` reads `result_fetch_chunk`. Its column readers are made from the result's schema before any
  chunk, so an empty result has its columns, and each chunk points them at its vectors (`Reset(_duckdb_v2_vector*)`).
  A vector that is not flat is flattened first (`vector_flatten`). The readers stay after the last result and after
  `Close()`, as with V1, because `DataTable.Load` asks for the schema then. An error in a later chunk is reported by
  `Read()`.
- `VectorDataReaderFactory.CreateReader(_duckdb_v2_logical_type*, ...)` maps V2's type ids to `DuckDBType`
  (`Execution/DuckDBV2TypeMap.cs`) and makes the readers of BOOLEAN, the integer types, FLOAT, DOUBLE, VARCHAR, BLOB,
  BIT and NULL. Any other type throws `NotSupportedException` until its step.
- `Cancel()` uses `connection_interrupt`; an interrupted query throws `OperationCanceledException` as before.
- Not migrated yet, and throwing `NotSupportedException` with a pointer here: commands with parameters and
  `Prepare()` (stage 3), `ExecuteArrowStream` and `ExecuteArrowBatchesAsync` (stage 7).
- Tests that left a reader open were fixed to dispose it (47 `using` added, a few explicit `Dispose()` calls, and a
  `Dispose` override in `DuckDBDataReaderTestAllTypes`). `V2QueryTests` covers the decisions above.
- Speed: reading 20 million rows of INTEGER, BIGINT, DOUBLE and VARCHAR took 527 to 655 ms with V1 and 564 to 687 ms
  with V2, the same nightly library, timed with a stopwatch rather than BenchmarkDotNet.

### Step 2: dates, times, decimals, wide integers, uuid, interval and enum (done)

- The readers of DATE, TIME, TIME_NS, TIMETZ, the TIMESTAMP kinds, INTERVAL, UUID, HUGEINT, UHUGEINT and BIGNUM only
  need a vector's data, so they moved as they were. TIME and TIMETZ are decoded with V1's `duckdb_from_time` and
  `duckdb_from_time_tz`, which take no handle.
- V2 has no getter per kind, so `Execution/DuckDBV2LogicalTypes.cs` reads a type's parameters: DECIMAL's width and
  scale as UINTEGER values, an ENUM's names as VARCHAR values, one parameter per entry. The storage follows from
  them as V2 states in its contract: a DECIMAL of width up to 4 is int16, up to 9 int32, up to 18 int64, beyond that
  int128; an ENUM of up to 255 entries is uint8, up to 65535 uint16, beyond that uint32. The enum reader keeps a copy
  of its type (`DuckDBV2LogicalType`) to look names up, cached as before.
- A type with no reader gets the base reader, as with V1: the query runs and reading that column throws.
  TIMESTAMP_TZ_NS and UNION are two; V1 reports TIMESTAMP_TZ_NS as 42, which `DuckDBType` has no member for, and so
  does V2 now. Only list, array, struct and map throw `NotSupportedException` until step 3.
- `GetSchemaTable` of an empty result now reports a DECIMAL's precision and scale. V1 took them from the first
  chunk's vector and reported 0 and 0 without rows; `ReadDecimalSchemaWithoutTableRow` now expects the real values.
- `V2TypeTests` reads DECIMAL and ENUM at every storage boundary. 2919 test cases are marked V2-ready.
- Speed: reading 10 million rows of DECIMAL(18,3), TIMESTAMP and ENUM took 231 to 410 ms with V1 and 218 to 365 ms
  with V2 after the first round, timed with a stopwatch.

### Step 3: list, array, struct and map

- Child types come from `logical_type_get_param` as TYPE values (`value_get_type`), and child vectors from
  `vector_get_child`. UNION is not migrated: DuckDB.NET has no reader for it.
- Tests: the list, map, struct and nested reader tests, `DuckDBDataReaderTestAllTypes` last. The reader benchmarks
  run before the stage is done: `DataChunk/Reader/DESIGN.md` has the performance constraints.

## Stage 3: parameters and prepared statements

- `ClrToDuckDBConverter` creates values with `value_create_*`, which needs the connection's factory. Enum and union
  values are made by casting.
- Parameters are passed as a value array to `statement_execute` or `prepared_statement_execute`, by position or by
  name. Their types come from `statement_bind`.
- Tests: `Parameters/*` and `PreparedCommandTests`.

## Stage 4: writing primitive types

- The writers size each vector with `vector_set_size`, write strings through the vector's arena and use the mutable
  validity mask. Primitive types first, then lists and structs.
- The appender builds chunks as it does today, appends them to a column data collection and, on flush, runs
  `INSERT INTO t (columns) SELECT * FROM buf` with `statement_add_collection`. The column types come from binding
  `SELECT * FROM t`. `append_default_to_chunk` has no equivalent, so the column list leaves out defaulted columns.
- Tests: `AppenderTests`, `DuckDBManagedAppender*`, `DuckDBMappedAppenderTests` and the `*WriteConversionTests`.

## Later stages

5. Scalar functions: `connection_register_scalar_function`, errors through the callback's `err` slot.
   `UdfExceptionStore` needs a new key, because V2 has no connection id. Overloads are probably registrations under the
   same name, which needs a test.
6. Table functions.
7. Arrow: results through `statement_execute_arrow`. `DuckDBArrowAppender` has no direct path in V2.
8. Cleanup: the connection string builder enumerates the options through V2, Data stops using V1, the fate of the V1
   declarations in Bindings is decided, and CI runs the whole test suite again.

## Gaps to raise with DuckDB before V2 is stable

- A way to get a V2 handle from a V1 connection, which would allow a gradual migration.
- Appender helpers: default values and column lists.
- Arrow import and export outside callbacks.
- A connection id that callbacks can read.
- Enum and union value constructors.
- An equivalent of `duckdb_get_table_names`.
- How overloads work without function sets.

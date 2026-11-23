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
- Each stage names the tests that must pass before the next one starts. From stage 1 on, CI on `nightly-vnext` runs
  only the test classes the finished stages have enabled, so the run stays green while the rest of Data is broken.

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

## Stage 1: open and close

- One environment for the process, created on first use and never destroyed: `environment_destroy` refuses while an
  instance is alive anyway.
- `ConnectionManager` keeps its cache and reference counting, because it also covers `:memory:?cache=shared`, which
  `instance_open_shared` never shares. `duckdb_create_config`, `duckdb_set_config` and `duckdb_open_ext` become
  `instance_create_with_options` (option names and values, `duckdb_api` among them) followed by
  `instance_attach(path or ":memory:", make_default: true)`. `duckdb_connect` becomes `connection_create`, for both
  `GetConnectionReference` and `DuplicateConnectionReference`.
- `ServerVersion` uses `library_version`.
- To check in this stage: whether `access_mode=READ_ONLY` still works as a startup option or has to become an attach
  option; that a bad option is reported for the whole create call, so V1's "Error setting 'x' to 'y'" message changes;
  whether settings such as `lock_configuration` are accepted before the attach.
- Tests: `OpenTests`, the open, close, state, clone and file parts of `DuckDBConnectionTests`,
  `DuckDBClientFactoryTests`, and the parsing tests in `ConnectionStringTests`. The tests that check a setting by
  running SQL move to stage 2.

## Stage 2: queries without parameters

- `DuckDBCommand` runs `parse_sql`, iterates the statements and calls `statement_execute`; the V1 `DuckDBResult`
  struct is replaced by a result handle. `ExecuteNonQuery` reads the changed rows with `result_drain`. With several
  statements, `NextResult` executes the next one only after the current one is finished.
- The one open result per connection rule needs a policy: starting a command while a reader is open fails with
  `RESOURCE_IN_USE`. The proposal is to throw a clear `InvalidOperationException`, as SqlClient does without MARS, and
  document it. The alternative is to drain the open reader into managed memory.
- `DuckDBDataReader` uses `result_fetch_chunk` and the result schema for the column names and types. Errors can now
  arrive while fetching, not only when the statement is executed.
- The vector readers get `view.data` and `view.validity`. A vector that is not FLAT is flattened with `vector_flatten`
  for now; reading through the selection vector can come later if the benchmarks justify it. Decimal width and scale,
  enum dictionaries and child types come from `logical_type_get_param`. The V2 type ids map to `DuckDBType`.
- Order: bool, integers, floats, varchar and blob; date, time and timestamp; decimal, hugeint, uuid and interval; enum;
  list, array, struct, map and union (whose tag is child 0).
- `connection_interrupt` for cancellation and `connection_progress_get` for the query progress.
- Tests: `QueryScalarTests`, `ExecuteNonQueryTests`, `DuckDBDataReaderTests`, `TransactionTests`, `SchemaTests`, then
  the reader and type tests in the order above, `DuckDBDataReaderTestAllTypes` last. The reader benchmarks run once
  before the stage is done: `DataChunk/Reader/DESIGN.md` has the performance constraints.

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

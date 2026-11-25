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
- Each stage names the tests that must pass before the next one starts. Until stage 7, a test that passed on V2 got
  `[Trait("V2", "Ready")]` and CI on `nightly-vnext` ran only those (the counts of "marked V2-ready" below are from
  then). Since then the whole suite runs, and a test that cannot pass yet is skipped with a reason from
  `DuckDB.NET.Test/V2/V2Skip.cs`: materialized results (a skipped `InlineData(false)` row of a `useStreamingMode`
  theory) and two threads on one connection. A skip goes when what it names is done.
- Code that is not migrated yet throws `NotSupportedException`: `UseStreamingMode = false`.

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
- `duckdb_create_config`, `duckdb_set_config` and `duckdb_open_ext` became `instance_create_with_options` (the
  connection string's options, `duckdb_api` among them) followed by `instance_attach(path or ":memory:",
  make_default: true)`; `ConnectionManager` kept its own cache, on the belief that `instance_open_shared` never shares
  `:memory:?cache=shared`. That was wrong: to DuckDB every `:memory:` followed by a name is a named in-memory database,
  shared like a file. Since then `OpenInstance` calls `instance_open_shared` (path, startup options) and
  `ConnectionManager` has no cache: DuckDB shares one instance per file, as it resolves the path, and per named
  in-memory database, waits for a closing instance before opening its file again, and refuses a database open with
  other startup options. Checked on v2.1.0-alpha57331: two spellings of one path share, `threads=3` matches
  `THREADS=03`, `access_mode=read_only` against an open read-write file is refused ("Can't open a connection to same
  database file with a different configuration than existing connections"), names are case-sensitive on Linux, and a
  named in-memory database is gone after its last connection. The refused-option search (`NameTheRefusedOption`)
  stays, and is skipped for that refusal, which is about the combination. No test changed; the connection stress
  tests pass. `duckdb_connect` became `connection_create`. `ConnectionReference` holds the `DuckDBV2Instance` an open
  created and its `DuckDBV2Connection`, and closing destroys both: with no duplicates, no handle is shared, so
  `FileReference` and its count are gone.
- `Duplicate()` is `[Obsolete]` and throws `NotSupportedException`. It existed to reach a private `:memory:` database
  from a second connection; V2 has no way to get an instance back from a connection, and a named in-memory database
  (`:memory:<name>`) does the same job. Its tests (`DuplicateInMemoryConnection`,
  `DuplicateInMemoryNotOpenedConnectionError`, `DuplicateUsesTheSameInstance` and the duplicate theory of
  `V2SharedInstanceTests`, and `DuplicateNotInMemoryConnectionError`) were removed. Its removal belongs in the
  release notes.
- `ServerVersion` uses `library_version`, and `GetQueryProgress` uses `connection_progress_get`.
- `DuckDBConnection.NativeConnection` was a V1 handle (`DuckDBNativeConnection`), which a V2 connection does not
  have, and `duckdb_v2.h` has nothing to make one from. It is now the connection's `duckdb_v2_connection_handle`, as an
  `IntPtr` (the V2 handle classes stay internal with the bindings): valid while the connection is open, not to be
  destroyed, and not to be used while a command or reader of the connection runs. It throws
  `InvalidOperationException` on a connection that is not open, which checks the state, not the reference: a closed
  connection keeps its reference, whose handle is destroyed. `V2NativeConnectionTests` pins it.
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

## Stage 2: queries without parameters (done)

Done in steps: first the execution path and the primitive types (step 1, done), then dates, times, decimals,
hugeint, uuid, interval and enum (step 2, done), then list, array, struct and map (step 3, done). UNION has no reader in DuckDB.NET, so it
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

### Step 3: list, array, struct and map (done)

- The LIST, ARRAY, STRUCT and MAP readers have a V2 constructor and a V2 `Reset`. Their child types come from
  `logical_type_get_param` as TYPE values (`value_get_type`): a LIST's or ARRAY's element type and an ARRAY's size
  (BIGINT), a MAP's key and value types, and a STRUCT's fields, each named after its parameter. The child readers are
  made by the same factory, so nesting goes as deep as the type does.
- `Reset` flattens the parent first, then takes the children with `vector_get_child`: [0] the elements of a LIST or
  ARRAY, [0] the entries of a MAP (a STRUCT of key and value, flattened before its own children are taken), [i] field
  i of a STRUCT. The code that builds .NET lists, dictionaries and objects did not change.
- UNION is not migrated: DuckDB.NET has no reader for it, so it keeps the base reader, as with V1.
- No query result we tried had a vector that is not flat: constants, cross joins, enums, Parquet, unnest. DuckDB
  appears to hand results out flat, so the flattening is a safety net for query results; it matters for function
  inputs (stage 5). `V2VectorTests` builds constant vectors through V2 and reads them, so it is tested.
- Left from stage 1: a file database is named after its file, as with V1 (`current_database()`), and an in-memory one
  `memory`. MotherDuck (`md:`) data sources go through `instance_attach`, which is not tested: it needs a token.
- `V2NestedTypeTests` reads deep nesting, NULLs at every level, many chunks and the same nested value in every row.
  3405 test cases are marked V2-ready.

## Stage 3: parameters and prepared statements (done)

- `PreparedStatement/DESIGN.md` ("On `nightly-vnext`") describes the V2 paths; in short:
  - A statement that declares parameters is bound without running (`statement_bind`) for its parameter schema, then
    executed with its values (`statement_execute`). The schema names each parameter, and the name is its binding key,
    so positional and named binding are one path.
  - `Prepare()` uses `prepared_statement_create` and `prepared_statement_execute`; the schema is read once.
  - Values are bound per execution, so there are no bindings to clear.
  - `ClrToDuckDBConverter` keeps its rules and creates values with the connection's factory
    (`Execution/DuckDBV2Values.cs`). A NULL is SQLNULL-typed, a UUID goes in DuckDB's flipped form, a BIGNUM as the
    bytes from `bignum_encode`, and lists copy their items.
  - V1's `PreparedStatement.cs` and `StatementResult.cs` are gone.
- **Behaviour change:** a connection has one open result, so two threads executing on one connection at once collide:
  the second is refused with `InvalidOperationException` (RESOURCE_IN_USE). V1 ran both. ADO.NET does not promise
  that a connection is thread-safe, but this worked before. `CommandsWithTheSameTextOnTwoThreads` pins the V1
  behaviour and is not marked V2-ready. Whether to serialize executions on a connection is open.
- Test changes: tests that left a reader open before the next command dispose it first;
  `PrepareThatFailsWhileAResultIsOpenIsRetried` expects `InvalidOperationException`; `ExecutingAgainWhileAReaderIsOpen`
  checks that the second execution is refused and runs once the first reader is closed. 3920 test cases are marked
  V2-ready (the theories with random data vary by a case or two).

## Stage 4: writing primitive types

### The vector writers (done)

- The writers take a V2 vector: `VectorDataWriterFactory.CreateWriter(_duckdb_v2_vector*, _duckdb_v2_logical_type*)`.
  The data comes from `vector_get_data_mutable` and the validity from `vector_flat_get_validity_mutable`, which
  allocates the mask. A writer that has a mask fetches it again when its vector moves; one that has none fetches it on
  the first NULL, as with V1. The V1 overload throws `NotSupportedException`: the appender and the functions still
  call it, and move to V2 in their own steps.
- Strings: a value of 12 bytes or fewer is inlined into the 16-byte slot and padded with zeros. A longer one is copied
  into memory from `arena_allocate` on the vector's arena, and the slot holds its length, its first 4 bytes and the
  pointer. V1 had `duckdb_vector_assign_string_element_len` do this.
- LIST and ARRAY: the items are child [0] and their type the type's first parameter; an ARRAY's size is its second
  (BIGINT). A LIST grows its items with `vector_set_size` on the items' vector, which reserves room, and sets the size
  that counts after each list. V1 used `duckdb_list_vector_reserve` and `duckdb_list_vector_set_size` on the list.
- ENUM and DECIMAL: V2 has no internal-type getters. The storage follows from the type as V2 states in its contract:
  an ENUM's index is uint8 up to 255 entries, uint16 up to 65535, uint32 beyond; a DECIMAL is int16 up to width 4,
  int32 up to 9, int64 up to 18, int128 beyond. The ENUM names are the type's parameters.
- STRUCT, MAP and BIT are not written, as with V1. UNION is not migrated.
- `V2WriterTests` writes a column of a data chunk created through V2 (`data_chunk_create`) and reads it back with the
  V2 readers: NULLs, short and long strings, an arena that needs more than one block, blobs, the four DECIMAL
  storages and a value too wide for its column, ENUMs with uint8 and uint16 indexes, LISTs that outgrow their items'
  vector, ARRAYs and an ARRAY of the wrong length. A round trip cannot catch a constant that is wrong in both
  directions; the `*WriteConversionTests` compare with DuckDB once the appender is on V2. 3420 test cases are marked
  V2-ready.

### The appender (done)

V2 has no appender. `DuckDBAppender` keeps its API and writes its rows into `Execution/DuckDBV2AppendBuffer`:

- **Buffer:** the writers fill a data chunk; full chunks go into a column data collection, which INSERT statements
  read as a table (`statement_add_collection`, `INSERT INTO t (columns) SELECT columns FROM __duckdb_net_appender`),
  prepared once per appender. The column names and types come from binding `SELECT * FROM t` when the appender is
  created, so a missing table is reported then. Names are quoted.
- **Flush:** every 204,800 rows, as DuckDB's appender did, and on `Close()`. A failed flush drops the rows since the
  last one (`AFailedFlushDropsTheRowsSinceTheLastFlush`). Clearing the collection invalidates its append state, so
  the state is made again after every flush and `Clear()`.
- **Defaults:** `AppendDefault()` writes a NULL and records the column in the row's pattern (the columns it leaves
  to their defaults). The collection has a hidden last column that numbers the rows. A flush of one run of one
  pattern is one INSERT of the pattern's columns; a flush of several runs inserts each run with `WHERE
  __duckdb_net_row >= $1 AND < $2`, so rows keep their order and each run leaves out its own columns. A row that
  leaves every column to its default is `INSERT ... DEFAULT VALUES`. A flush of several runs is several statements:
  outside a transaction, a failure part way keeps the runs before it.
- **Behaviour changes:** a flush while a reader is open on the connection is refused with
  `InvalidOperationException` (one open result per connection). The errors of a flush are those of the INSERT.
- **Not measured:** the path is chunk, collection, INSERT, where V1 appended to the table's storage directly. The
  list appender tests take 2 to 4 s each; there is no V1 time to compare them with yet. To benchmark on a VM.
- Not handled: a table with generated columns (`SELECT *` includes them, an INSERT cannot write them), and
  `preserve_insertion_order = false`, under which the INSERT need not keep the order of the buffer.
- `V2AppenderTests` pins the default patterns over chunk boundaries, a row of only defaults, the flush at 204,800
  rows, a failed flush, an open reader, quoted names and a missing table. `DuckDBArrowAppender`: see stage 7. 4292 test cases are marked V2-ready.

## Later stages

5. Scalar functions (done): `connection_register_scalar_function`, errors through the callback's `err` slot.
   - **Registration** (`Functions/ScalarFunctionRegistration.cs`, since develop moved the registration into
     `Functions/`; the public methods pass `V2Connection` on): positional-only parameters named `arg0`, `arg1`, ...
     (invisible to SQL, but they may not share a name), or one `POSITIONAL_VARIADIC` parameter for `params`. Volatile
     and special NULL handling are the properties `STABILITY_VOLATILE` and `NULL_HANDLING_SPECIAL`; fallibility keeps
     V2's default, `FALLIBLE`. The action and the types are the function's user data, which DuckDB destroys with it.
   - **Overloads:** a second function with the same name and other parameter types is an overload. Registered again
     with the same signature, a function replaces the first, with no error. `V2ScalarFunctionTests` pins both.
   - **Arguments and result:** the stage 2 readers read the arguments (`exec_get_arg`) as the parameter types,
     which DuckDB casts them to; an argument of an ANY parameter (`object`) is read as the type its vector has. The
     stage 4 writers write the result (`exec_get_result`).
   - The types come from `DuckDBV2Types.ForClrType`, so `ScalarFunctionRegistration.Register` takes CLR types.
   - The bind callback and the exception slot are in `Connection/UdfErrors.cs`, which develop introduced for
     ExtensionKit: on V2 it uses `DuckDBV2CallbackScope` instead of `UdfExceptionStore`.
   - Named parameters are not registered yet: V1 scalar functions had none. When they are added, they follow
     "Named parameters and defaults" below, as table functions do.
   - Not measured against V1 yet: the readers are made per chunk, as with V1, now from V2 types.
   - **The exception of a C# UDF** reaches the command without passing through the error text. V2 has no
     connection id, and a callback cannot see its connection, so `UdfExceptionStore` cannot keep its key. Instead the
     connection puts itself in a `[ThreadStatic]` before each native call that can bind; the function's bind
     callback hands the connection's exception slot to the exec callbacks as bind data (`bind_set_bind_data`,
     `exec_get_bind_data`); the exec callback stores the exception there and sets only DuckDB's error code and text,
     and the command attaches it as `InnerException`. That also ends the flaky failures from connection ids that
     repeat across databases.
   - **Checked first** (`V2ScalarBindProbeTests`, 5 runs): bind runs on the calling thread for an unprepared query,
     at `Prepare()` (twice) and again at every execution of a prepared statement with parameters, and after the
     table under a prepared statement is replaced. Exec runs on up to four threads, sometimes not on the caller's, and
     always sees the bind data of the current execution. An error the exec callback sets reaches the command as
     "Invalid Input Error: <text>". The bind data is destroyed.
   - **The slot** is `DuckDBV2Connection.CallbackException`, and `DuckDBV2CallbackScope` is the `[ThreadStatic]`. An
     error DuckDB never reports can leave an exception in it (`TRY(f(x))` turns the error into NULL), so each
     execution clears it (`EnterExecution`). `ThrowIfFailed(ref error, connection)` attaches it, on executing,
     preparing, fetching and draining. `V2ScalarFunctionTests` checks a function registered on one connection and run
     on another of the same database, a swallowed error, and every execution of a prepared statement.
   - 3990 test cases are marked V2-ready. The scalar function tests left out need the appender or materialized
     results.
6. Table functions (done), registered in `Functions/TableFunctionRegistration.cs`. Named parameters follow "Named parameters and defaults" below. The rest maps one to one:
   - **Scan:** V2 scans on one thread and keeps insertion order by default, as V1 does; the data enumerator is not
     thread-safe, so both stay. It is still created per scan init (the global init callback), not per bind, for
     recursive CTEs.
   - **Output and pushdown:** the output chunk comes from `exec_get_output_chunk`, its row count from
     `vector_set_size` on its first vector. Projection pushdown and cardinality have direct calls. `count(*)` still
     gets one column from DuckDB (`AQueryThatReadsNoColumnCountsTheRows`); with none there is no vector to size, and
     the code skips it.
   - **Exceptions:** through the bind data, as for scalar functions; the bind callback is ours, so the connection
     goes next to the function's data.
   - **Parameter values:** V2 hands values (`function_bind_get_arg_value`), read by `DuckDBV2ValueReader`, which has
     the conversions V1's `DuckDBValue.GetValue<T>` had, case for case (`DuckDBValue` is gone, see "V1 code that is
     gone"). A LIST parameter cannot be read, as with V1.
   - **Named parameters** are registered `NAMED_ONLY`, with the default from `[Named(Default = ...)]`, cast at
     registration; positional ones `POSITIONAL_ONLY`. Changed tests: `RegisterTableFunctionNamedParameter` and
     `...MultipleNamedParameters` declare `Default = null`; `...NonNullableThrows` expects DuckDB's "Required parameter
     "limit" is missing". New: `ANullableNamedParameterWithoutADefaultIsRequired`, `NamedParametersTakeTheirDefaults`,
     `ADefaultThatIsNotOfTheParameterTypeIsRefusedAtRegistration`, `ACSharpDefaultOnANamedParameterIsRefused`.
   - `UdfExceptionStore` is no longer written to; the V1 Arrow stream still reads it until stage 7. 4081 test cases
     are marked V2-ready.
7. Arrow.
   - **Results (done):** `ExecuteArrowStream` and `ExecuteArrowBatchesAsync` execute with `statement_execute_arrow`
     (`prepared_statement_execute_arrow` after `Prepare()`), and `DuckDBArrowArrayStream` imports each array that
     `arrow_result_fetch_array` returns. DuckDB makes the batches, of V2's default size, up to 131,072 rows (V1: one
     per chunk of 2,048 rows). The statements before the first that returns rows are drained. A failure is a
     `DuckDBException` with DuckDB's error type, and a C# function's exception as the inner exception
     (`V2ArrowResultTests`); the C stream that `arrow_result_to_arrow_c_stream` makes would report only EIO.
     `UseStreamingMode = false` throws, as for readers. `UdfExceptionStore` and the V1 result extensions are gone.
   - **`DuckDBArrowAppender` (done):** V2 has no appender for Arrow data, and its Arrow importer
     (`arrow_importer_create`) needs a context with an active transaction, which only callbacks are given. So each
     batch is one `INSERT INTO table SELECT * FROM __duckdbnet_arrow_append($1)`, prepared once per appender, with the
     appender's id as the parameter (`Execution/DuckDBV2ArrowAppend.cs`):
     - The bind callback creates the importer, once per appender and `Schema` object (as V1 converted the schema
       again for another `Schema`), with a batch size of 2048, the output chunk's capacity, and declares the columns
       from it. Within one connection the context stays the same, so the importer serves every later INSERT.
     - The exec callback appends the batch with `consume` (DuckDB takes the exported array) and `flush`, and
       references each chunk's vectors into the output (`vector_reference`): nothing is copied before DuckDB writes the
       table. DuckDB releases the array before the INSERT returns, so a batch can be disposed then.
     - The function is registered in the `init` callback of `instance_open_shared`, once for every instance DuckDB
       creates, before another open can share it. Registered later, it would be refused the second time ("function
       already exists"), be part of the user's transaction (invisible to other connections until it commits, undone by
       a rollback) and collide with another connection's registration ("Catalog write-write conflict"). A failed
       registration does not fail the open; only the Arrow appender then fails. An id that is no appender's is refused,
       so SQL cannot make the function read memory.
     - Checked on v2.1.0-alpha57486 with a probe: bind runs once per execution, not at `Prepare`, and sees the
       parameter; a 3000-row batch comes out as two chunks; int32 is cast to a BIGINT column; registering in `init`
       costs nothing measurable next to the 14 ms an in-memory instance takes to open.
     - **What changed from V1:** a batch is in the table when `AppendBatch` returns, with no flush; a batch that fails,
       a constraint included, is thrown by its own `AppendBatch`, drops only itself and faults the appender, where V1
       checked constraints at a flush and dropped everything since the last one; `Clear` does nothing, and is
       `[Obsolete]`. `AppendStream`
       and `AppendStreamAsync` insert batch by batch, as before. Changed tests:
       `AppendBatch_ConstraintFailureKeepsEarlierBatches` and `AppendBatch_ConstraintFailureFailsTheBatchThatBreaksIt`
       (were `AppendBatch_ConstraintFailureAtFlushDropsEarlierBatches` and
       `Dispose_ConstraintFailureDropsUnflushedBatches`); `Clear_DropsAppendedBatches` and the `Clear` line of
       `AppendBatch_FailedCastFaultsAppenderAndKeepsEarlierRows` are removed with `Clear`. `V2ArrowAppenderTests` pins
       the rest.
     - `ArrowAppenderBenchmark.NativeCalls`, which made V1's calls directly, is gone; the other two benchmarks measure
       the V2 appender. Not measured yet against V1.
8. Cleanup: the connection string builder enumerates the options through V2, Data stops using V1, the fate of the V1
   declarations in Bindings is decided, and CI runs the whole test suite again.

## V1 code that is gone

Everything of the V1 C API that nothing outside the tests used is removed from the bindings: 265 of the 293
`NativeMethods` declarations, `DuckDBValue`, `DuckDBLogicalType`, `DuckDBConfig`, `DuckDBExtractedStatements`,
`DuckDBArrowOptions`, `DuckDBClientContext`, `DuckDBBignum`, `DuckDBBlob`, `DuckDBResultType` and
`DuckDBNativeConnection.Interrupt`, and, once the Arrow appender stopped using them, the V1 appender, Arrow,
data chunk and error data functions with their handles (`DuckDBAppender`, `DuckDBDataChunk`,
`DuckDBArrowConvertedSchema`, `DuckDBErrorData`), `DuckDBStateExtensions`, `DuckDBCallerOwnedStringMarshaller` and
`duckdb_free`. They were public in `DuckDB.NET.Bindings`, so this is a breaking change of that
package for the release notes. Tests that tested the removed code went with it: `AppenderTests` and `QueryTests` (the
V1 appender and query API), `DuckDBValueGetValueTests`, and `TimeNsDuckDBValueIsReadWithTimeOnlyPrecision`
(`DuckDBValue`); `V2ValueReaderTests` pins the parameter conversions on V2.

What is left, and why:

- **The configuration options** for the connection string builder (`duckdb_config_count`, `duckdb_get_config_flag`;
  V2's list misses the extension settings, see the gaps below).
- **The low-level sample** in `DuckDB.NET.Samples` (`duckdb_open`, `duckdb_connect`, `duckdb_query`, `duckdb_prepare`,
  `duckdb_bind_int32`, ...), with the handles `DuckDBDatabase`, `DuckDBNativeConnection` and
  `DuckDBPreparedStatement`.
- **References for tests:** `duckdb_to_time`, `duckdb_create_time_tz`, the five `duckdb_is_finite_*` functions,
  `duckdb_library_version`, and `duckdb_from_date`, `duckdb_to_date`, `duckdb_from_timestamp` and
  `duckdb_to_timestamp`, which the tests check the provider's conversions, infinity values and version against.
  Only the tests use them; V2 has no equivalent but `library_version`.

The date and timestamp conversions of `DuckDBDateOnly` and `DuckDBTimestamp` used the last four. They are C#
arithmetic now (`DuckDBTimeConversions`: Howard Hinnant's `civil_from_days` and `days_from_civil`), with V1's results
for what does not exist or does not fit, checked against it on v2.1.0-alpha57631: a date that does not exist (a
30 February, a 29 February outside a leap year, a month 13) or is outside the range of a DATE gives day 0, and a
timestamp gives 0 for such a date and for a sum that does not fit or is one of the two infinities, while the time of
day is not checked. `DuckDBDateConversionTests` pins them against V1's functions.

## Named parameters and defaults

Decided for table functions (stage 6), and to apply to scalar functions when they get named parameters: V2 scalar
functions can have them (`STANDARD` and `NAMED_ONLY` parameter kinds), V1 ones could not, and stage 5 registers only
positional ones.

### The rule

**A named parameter is required unless the C# code gives it a default.** With V1 an omitted named parameter reached
the function as NULL: a nullable parameter could not tell "left out" from "passed NULL", and a forgotten or misspelt
argument became `null` without a word. V2 enforces required parameters itself, so:

- **No default:** registered without one (`NAMED_ONLY`, `value` NULL). DuckDB refuses a call that leaves it out, at
  bind, before any C# code runs. This holds for nullable parameters too.
- **A C# default** (`[Named] int limit = 100`, `[Named] string? suffix = null`): registered with that value as the
  default, which DuckDB fills in. `= null` is how an author says "optional, NULL when left out".
- **An explicit NULL** (`prefix := NULL`) for a non-nullable parameter still throws, as with V1.

**Breaking change** (for the release notes of the switch to V2): a nullable named parameter without a default, left
out of a call, used to arrive as `null` and is now an error. The fix is `= null` on the parameter.

### What DuckDB does (checked, `V2NamedParameterProbeTests`)

- A required named parameter left out is a Binder Error that names it: "No function matches ... Candidates:
  f(x INTEGER, /, *, n INTEGER) -> INTEGER / Required parameter "n" is missing".
- An explicit NULL is passed to the function; a NULL default (`value_create_null`) is cast to the parameter's type
  and arrives as NULL.
- Names match without regard to case (`N := 6` reaches `n`).
- **A default is cast to the parameter's type when the function is called, not when it is registered.** A VARCHAR
  default becomes a DATE ('2024-01-01'), a UUID or an INTEGER[] ('[1, 2, 3]'), with no SQL quoting: it is a string
  value, not SQL text. A default that does not cast ('not a date' for a DATE) is accepted by the registration and
  fails every call that leaves the parameter out ("Conversion Error: invalid date field format"). So the
  registration has to check a default itself, by casting it, and refuse the function.
- `duckdb_functions()` lists the parameters and types; it shows no defaults.

### What C# and reflection give

- **Defaults are compile-time constants only:** numbers, `bool`, `char`, `string`, enums, `decimal` and `null`.
  `DateTime`, `DateOnly`, `TimeOnly`, `TimeSpan`, `DateTimeOffset`, `Guid`, `BigInteger`, arrays and lists can only
  have `default`, their zero value. `[Optional, DateTimeConstant(ticks)]` gives a `DateTime` a real one (not tried on
  a lambda).
- **Reflection** (`ParameterInfo.HasDefaultValue`, `DefaultValue`) reports a lambda's defaults, also when the lambda
  is converted to a `Func<...>`, which is what the public API takes.
- **`default` of a struct reads as null:** for `DateTime when = default` or `Guid id = default`, `DefaultValue` is
  null, as for `string none = null`. A non-nullable value type whose `DefaultValue` is null therefore means the zero
  value of the type (`Activator.CreateInstance`), not NULL.
- **The compiler warns.** A default in a lambda converted to `Func<...>` is warning CS9099 ("Parameter 2 has default
  value '"Mr."' in lambda but '<missing>' in the target delegate type"), on every such parameter. Users would see it
  or suppress it, which argues for an attribute instead; see the open question below.

### Decided: defaults are written in the attribute

`NamedAttribute` gets a `Default` property. C# defaults on the lambda are not used: each one is warning CS9099, and
they allow constants only.

```csharp
(int count,
 [Named] string prefix,                          // required
 [Named(Default = null)] string? suffix,         // optional, NULL when left out
 [Named(Default = 100)] int limit,               // optional, a constant of the parameter's type
 [Named(Default = "2024-01-01")] DateOnly from,  // optional, text that DuckDB casts to the type
 [Named(Default = "[1, 2, 3]")] List<int> ids) => ...
```

- **`Default` is `object?` with a setter that also records that it was set**, so `Default = null` (optional, NULL)
  differs from no `Default` (required). An attribute argument cannot be a `decimal`, a date or a Guid, so those are
  written as text.
- **The value:** a string goes to DuckDB as VARCHAR, and DuckDB casts it to the parameter's type; any other
  constant goes through the parameter converter of stage 3 (`ClrToDuckDBConverter`) with the parameter's type.
- **Checked at registration:** the registration casts the default to the parameter's type with `value_cast`, and
  refuses the function if the cast fails, naming the parameter and the default. It registers the cast value, so a
  call never meets a default that does not cast (DuckDB itself casts only when the function is called).
- **A C# default on a named parameter is refused** at registration, with a message that points to
  `[Named(Default = ...)]`, so it is not silently ignored.
- **Positional parameters** stay required; defaults are for named ones only.

## Gaps to raise with DuckDB before V2 is stable

- The option list: `config_get_option_count` lists the core options and those of loaded extensions, so the
  connection string builder keeps V1's `duckdb_config_count`/`duckdb_get_config_flag` (no connection needed), which
  also list the settings of autoloadable extensions (`EXTENSION_SETTINGS`, about 150, e.g. `s3_region`). On V2's
  list alone, a connection string with such a setting would be refused.

- A way to get a V2 handle from a V1 connection, which would allow a gradual migration.
- Appender helpers: default values and column lists.
- Arrow import and export outside callbacks.
- A connection id that callbacks can read.
- Enum and union value constructors.
- An equivalent of `duckdb_get_table_names`.
- How overloads work without function sets.

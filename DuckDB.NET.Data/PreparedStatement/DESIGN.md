# Design of command execution and parameters

This file records how a command runs its text, how parameter values reach DuckDB, and why the code in this folder is
written the way it is. It covers both paths: the one every command takes, and the one a command takes after
`Prepare()`. Read it before changing `DuckDBCommand`'s execution, the prepared statement store on
`DuckDBConnection`, `PreparedStatement`, `PreparedStatementEntry`, `ClrToDuckDBConverter` or `DuckDBTypeMap`.

The DuckDB behaviour described here is that of v2.0.0 (`S:\src\duckdb-2`, read with `git show v2.0.0:<path>`). The
numbers were measured in November 2026 on the Azure benchmark VM with BenchmarkDotNet; the ratios should hold, the
absolute times will not.

## How a command executes

`ExecuteNonQuery`, `ExecuteReader` (and `ExecuteScalar` through it) and `ExecuteArrowStream` all call
`DuckDBCommand.ExecuteStatements()`, which returns a lazy sequence of `StatementResult` (a native result and the type
of the statement that produced it). It picks one of two paths:

```
ExecuteStatements()
  not prepared  -> PreparedStatement.PrepareMultiple(text)    every command, every execution
  prepared      -> ExecutePrepared()                          after Prepare()
```

### The unprepared path: `PrepareMultiple`

An iterator. One `MoveNext()` runs one statement:

```
duckdb_extract_statements(text)                      split and parse, once per execution
for each statement:
  duckdb_prepare_extracted_statement                 DuckDB binds and plans it
  duckdb_prepared_statement_type
  BindParameters(statement, parameters, entry: null)
    duckdb_nparams
    per parameter: duckdb_bind_parameter_index (named) and duckdb_param_logical_type
    ClrToDuckDBConverter.ToDuckDBValue -> duckdb_bind_value
  duckdb_execute_prepared[_streaming]
  yield the result
  destroy the statement                              when the caller moves on, or disposes the iterator
```

Statements are prepared one at a time, just before each runs, because a later statement can depend on an earlier one
having run (`CREATE TABLE t ...; INSERT INTO t ...`). The statement of a yielded result lives until the next
`MoveNext()` or until the iterator is disposed; `DuckDBDataReader.Close()` disposes the iterator after closing its
result, and before `CommandBehavior.CloseConnection` closes the connection (see the close-order rule in `AGENTS.md`).

### The prepared path: `ExecutePrepared`

Also an iterator, for one statement:

```
if Prepare() failed, or the entry is stale (destroyed, or replaced by a newer Prepare()):
  entry = connection.GetOrPrepareStatement(text)
if there is no entry (several statements) or entry.TryRent() fails (another execution holds it):
  fall back to PrepareMultiple
else:
  entry.Execute:
    duckdb_clear_bindings
    BindParameters(statement, parameters, entry)    count, types and name indexes from the entry
    duckdb_execute_prepared[_streaming]
  entry.Return()                                    in a finally, before the yield
  yield the result
```

Nothing is held across the `yield`: a result does not need its prepared statement, so the statement is free again
before the caller reads a row.

Both paths are iterators on purpose. Every error, from extracting, preparing, binding or executing, is raised on the
first `MoveNext()`, which `DuckDBDataReader` makes inside its constructor. The constructor then closes what it holds,
and the `CloseConnection` behaviour closes the connection, whichever path failed.

### Results

`StatementResult` carries the statement type because since DuckDB 2.0 a result of a prepared statement reports
`EXECUTE` (`duckdb_result_statement_type`); the type is taken from the prepared statement instead.
`DuckDBCommand.CountsRows` uses it: INSERT, UPDATE, DELETE, COPY and MERGE add their affected rows, every other
statement reports -1. `ExecuteNonQuery` and `DuckDBDataReader.RecordsAffected` sum over the statements.

## Binding parameters

`PreparedStatement.BindParameters` runs on every execution, on both paths. With an entry, the parameter count, each
parameter's logical type and the index of each name come from the entry; without one they are asked of DuckDB.

1. **Count.** Fewer values in the collection than DuckDB expects is an `InvalidOperationException`. More is allowed.
2. **Named or positional.** If any parameter in the collection has a name, every parameter is bound by name, and a
   name the statement does not declare is skipped. Otherwise the first `ParameterCount` values are bound by position,
   from 1.
3. **Type.** Each value is converted to the type DuckDB reports for its parameter (below), then bound with
   `duckdb_bind_value`. A failed bind is an `InvalidOperationException` with DuckDB's message.

DuckDB checks what was bound only when the statement executes (`PreparedStatementData::Bind`): a parameter left
without a value is an error naming it, and every value is cast with `DefaultTryCastAs` to the parameter's type. A
value that cannot be cast is "Type mismatch for binding parameter ...". So the converter does not have to produce
exactly the parameter's type: it has to produce a value DuckDB can cast to it, losing nothing on the way.

The loops index the typed collection instead of using `foreach` or LINQ, which allocated on every execution.

## Converting a value: `ClrToDuckDBConverter`

`ToDuckDBValue(value, logicalType, duckDBType, dbType)` turns a .NET value into a `DuckDBValue`. Its inputs:

- **The target type**, the logical type and type id DuckDB reports for the parameter. When DuckDB cannot infer one
  (an *untyped* parameter: `SELECT $1`, `$1 + 1`, `price * $1`, `$1 = $2`), the type id is `Invalid`.
- **The `DbType`** of the `DuckDBParameter`. Setting `Value` sets `DbType` to the one `DuckDBTypeMap` gives for the
  value's CLR type (`Object` for an unknown type or null), so a `DbType` set before `Value` is overwritten. The
  constructors that take a `DbType` keep it.

The decision order, first match wins:

1. **Null** (`null` or `DBNull`): SQL NULL.
2. **`Int128`, `UInt128`:** HUGEINT and UHUGEINT whatever the target, because neither is `IConvertible` and the integer
   targets below would send them as text.
3. **The target type and the CLR type together**, in the `switch`:
   - integer targets convert any value with `Convert.ChangeType` (current culture for strings), and send the
     value's text when that fails, for example out of range;
   - BOOLEAN, FLOAT, DOUBLE, DECIMAL, HUGEINT, UHUGEINT, BIGNUM, VARCHAR, UUID and BLOB take their own CLR type;
   - dates, times and timestamps take `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan` and the
     `DuckDB*` structs, converted to the precision of the target (`TIMESTAMP_S`, `_MS`, `_NS`, `TIME_NS`, ...);
   - LIST and ARRAY targets take an `ICollection`, each item converted to the child type with the same rules;
   - an untyped target with a `TimeSpan` gives an INTERVAL, unless the `DbType` is `Time`.
4. **Any other `ICollection`** (not `byte[]`): a LIST whose item type comes from the collection's element type.
5. **By `DbType`** (`ValueCreators`): the value as its own type. This is the path for untyped parameters, and for a
   value whose CLR type does not match the target (a `double` for a DECIMAL parameter): DuckDB casts it at execution.
6. **Anything else:** the value's `ToString()` as VARCHAR, for DuckDB to cast.

Details that matter:

- **Decimals** go as the integer and scale of the `decimal`, with the width of its digits, not as text or double, so
  no digit is lost. DuckDB casts to the parameter's width and scale. Building the value with the parameter's width
  and scale instead was tried during the prepared-statement work and dropped: no measurable effect on 2.0.
- **`BigInteger`** with no target goes as the smallest of HUGEINT, UHUGEINT and BIGNUM that holds it exactly.
- **Strings** are encoded to UTF-8 here and passed with their length, so a NUL character is kept.
- **Lists and arrays:** DuckDB returns no value when an item cannot be converted to the item type, and
  `duckdb_bind_value` would crash on it. `ThrowIfNotCreated` releases the items and throws a `DuckDBException`.
- **Logical types** passed in are borrowed: the converter must not dispose them. Child types it fetches
  (`duckdb_list_type_child_type`) are its own and are disposed.

Where the result types are tested: `DuckDB.NET.Test/Parameters` (one class per type family, `ExplicitCastTests`,
`ListParameterTests`, `ParameterCollectionTests`) and `PreparedCommandTests` for typed and untyped parameters of a
prepared command. For users, `docs/type-mapping.md` in the docs repository.

## What DuckDB does with a prepared statement (2.0)

Measured with the probe in `S:\src\DuckDB.NET.probes\prepare-probe` and read in the source:

- `duckdb_prepare` runs `PREPARE <generated name>`, and each execution runs `EXECUTE`.
- **A statement that reads a table is bound and planned again on every execution, by design.** `optimizer.cpp`
  sets `always_require_rebind` for a prepared plan with a `LOGICAL_GET` that reads a database (duckdb#24696, so
  that a plan follows the current statistics). A statement without a table scan (an expression, `INSERT ... VALUES`)
  keeps its plan, but v2.0.0 still runs the optimizer over the empty `EXECUTE` node; duckdb#27506 fixes that after the
  release, expected in 2.0.1.
- **An untyped parameter re-plans on every execution**: the plan depends on the type of the bound value.
- **`duckdb_param_logical_type` answers from a snapshot taken at prepare time.** While a value is bound, an untyped
  parameter reports that value's type; after `duckdb_clear_bindings` it reports no type again.
- A prepared statement holds a weak reference to its client context: it does not keep the native database alive
  after its connection is closed. A partly read streaming result does.
- While a streaming result is open on a connection, preparing or executing anything else on it fails with
  "connection has an open result" (1.x closed the stream instead).

So on 2.0 `Prepare()` saves the extraction, the parse and the plan made while preparing (the unprepared path plans
twice per execution: once in prepare, once in execute), plus the type lookups. It does not save the plan of a
statement that reads a table.

## Prepared commands

### Who owns what

- **`DuckDBConnection`** owns the statements: a dictionary from command text (ordinal) to `PreparedStatementEntry`,
  and a list of replaced entries that were still executing when they were replaced. One lock guards both.
- **`DuckDBCommand`** holds a reference to its entry (`preparedEntry`) and two flags: `prepared`, and
  `preparePending` (the last `Prepare()` failed).
- **`PreparedStatementEntry`** wraps one native statement with what binding needs: the statement type, the parameter
  count, each parameter's logical type and type id (read before any value is bound), and a cache of name-to-index
  lookups (a name the statement does not declare is cached as 0).

Commands with the same text on one connection share one entry. Another connection, or `Duplicate()`, has its own
store.

### Lifecycle

| Event | Effect |
|---|---|
| `Prepare()` | Prepares the text anew, makes the new entry the connection's, and retires the one it replaces. Other commands holding the old entry see it as stale and take the new one on their next execution. |
| `Prepare()` fails | The connection drops its entry for the text (it no longer matches the database), and the command stays prepared with `preparePending`: its next execution prepares again, reporting the error if it remains. |
| `Prepare()` on several statements | No entry; every execution takes `PrepareMultiple`. |
| `CommandText` changes (ordinal), `Connection` changes (reference) | The command is no longer prepared. |
| Command `Dispose()` | Nothing: the entry belongs to the connection. |
| Connection `Close()` | Every entry, kept or replaced, is destroyed before `ReturnConnectionReference`. Commands see their entry as stale; after a reopen the next execution prepares on the new native connection. |

### One execution at a time: rent, retire, return

DuckDB does not allow binding one prepared statement from two threads, and a shared entry can be reached from two
commands on two threads of one connection. So:

- **`TryRent()`** is a compare-and-swap of `inUse` from 0 to 1. A caller that loses takes `PrepareMultiple`, as an
  unprepared command would.
- **`Return()`** sets `inUse` back to 0 with `Interlocked.Exchange`, then, if the entry was retired meanwhile and it
  can rent it again, destroys it. The exchange is a full fence: the read of `retired` must not move before the
  write, or `Retire` on another thread could see the entry still taken while `Return` sees it not yet retired, and
  nobody would destroy it.
- **`Retire()`** (on the connection, under its lock) sets `retired`, then destroys the entry if it can rent it;
  otherwise it parks it in the replaced list, after removing the entries in it that are already destroyed.
- **`Dispose()`** runs once, guarded by `Interlocked.Exchange`. Only a thread holding the rent destroys an entry,
  except `Close()`.

A stress probe (3 executing threads, a 4th calling `Prepare()` 29,681 times on the same text) gave no wrong results,
no exceptions and no statement left undestroyed.

## Decisions

### 1. The store is on the connection, filled only by `Prepare()`

Statements are kept per connection and shared by text, like Npgsql. The alternative, the command owning its
statement with weak references from the connection (PR #340), ties the statement's life to the command's and needs
the connection to find live commands when it closes. With the connection owning everything, `Close()` has one place
to destroy from, before the native database can go. Preparing automatically (every executed text) is left for later.

### 2. Entries live until `Close()`

No reference counts on entries, no LRU, no removal when a command is disposed, no `Unprepare()`. Simple, and a
connection that prepares many different texts keeps all of them until it closes. The docs say so.

### 3. Each `Prepare()` prepares anew

A first version returned the kept entry when the text was already prepared. A review showed that this left no way to
refresh a statement after a change DuckDB does not track for it (a column type, `CREATE OR REPLACE VIEW` or `MACRO`,
`SET`) short of closing the connection, and the documented advice "call `Prepare()` again" did nothing. Now each call
prepares, and the old entry is retired. Code that creates a new command and calls `Prepare()` on every call prepares
every time, the same work as before; commands kept across executions get the gain.

Rejected: a counter of DDL and `SET` statements on the connection or the `FileReference`, to re-prepare when it moves.
It only sees statements sent through DuckDB.NET on that database, and costs a check on every execution.

### 4. A failed `Prepare()` keeps the command prepared

A failure can pass (another result was open on the connection). Unpreparing would silently send every later
execution down the slow path. The connection still drops its entry for the text, because the failure can also mean
the old statement no longer matches the database (a view it reads was dropped).

### 5. Types are read once; bindings are cleared before every bind

Reading the parameter types and the name indexes once is the saving. Clearing the bindings first is what keeps them
right: without it, an untyped parameter reports the previous value's type, and a named parameter missing from the
collection keeps its previous value instead of being reported missing.

### 6. Several statements are not prepared

A later statement may need an earlier one to have run before it can be prepared. A possible approach for later: keep
the handles prepared during the first run when every statement is SELECT, INSERT, UPDATE, DELETE or MERGE, using
`duckdb_prepared_statement_type` (the result's own statement type is `EXECUTE` on 2.0).

### 7. No schema tracking

A prepared statement keeps the parameter types, view and macro definitions and settings of the moment it was
prepared; the remark on `Prepare()` says to call it again after changing them. DuckDB re-binds a statement that reads
a table, but `duckdb_param_logical_type` still answers from the snapshot, so re-reading the types would not help.

### 8. No per-parameter binder delegates

Choosing a conversion delegate per parameter at prepare time would skip the `switch` on each bind. Left out: the
cached types already go into `ToDuckDBValue` unchanged, and the `switch` is not where the time goes.

### Smaller decisions

- `duckdb_clear_bindings` is not marked `[SuppressGCTransition]`: it frees every bound value, of any size, and the
  bind calls beside it are not marked either.
- `Prepare()` reports errors in the text as a `DuckDBException`; before, it only checked the connection.
- Text with no statement gives "The command text holds no statement to prepare".

## Measurements

`CommandBenchmark`, Azure VM, .NET 10, DuckDB 2.0.0, before and after the prepared path; 1.5.6.1 runs the 1.5.6
engine, where `Prepare()` did nothing.

| Benchmark | 1.5.6.1 | before | after |
|---|---:|---:|---:|
| ReusedCommand (primary key lookup, reads a table) | 182 us | 435 us | 191 us |
| ReusedExpression (`SELECT $1::INTEGER + 1`) | 105 us | 252 us | 42 us |
| ReusedInsert (two-parameter INSERT) | 217 us | 317 us | 80 us |

The benchmarks that do not call `Prepare()` stayed within 2.5%. Compare inside one job (`SelectExpression` against
`ReusedExpression`, `InsertRow` against `ReusedInsert`): the baseline also differs by engine.

## Known limits

- **Stale parameter types after a type-changing DDL.** A prepared `INSERT INTO t VALUES ($1)` with `v INTEGER`, then
  `ALTER TABLE t ALTER v TYPE DECIMAL(10,2)`: 1.75 is converted for an INTEGER and stored as 2.00 until `Prepare()` is
  called again. Unprepared stores 1.75.
- **The store grows until `Close()`.**
- **A second thread on one connection** executing the same prepared text at the same moment takes the unprepared
  path for that execution.
- **A replaced entry still executing on another thread** is destroyed when that execution gives it back, or at
  `Close()`.

## Rules for changing this code

- Keep `ExecutePrepared` an iterator, and keep `Return()` in a `finally` before the `yield`.
- Clear the bindings before binding, and never read parameter types while a value is bound.
- `Close()` destroys the statements before `ReturnConnectionReference`.
- Only a thread holding the rent destroys an entry (except `Close()`); keep the full fence in `Return()`.
- `ToDuckDBValue` never returns an invalid value: `duckdb_bind_value` does not check it.
- `BindParameters` runs on every execution: no allocations per execution.
- A new conversion needs tests in `DuckDB.NET.Test/Parameters` for a typed and an untyped parameter, and an update to
  `docs/type-mapping.md`.
- The tests in `PreparedCommandTests` that guard the store fail when the code they guard is removed; keep it that
  way for new ones.

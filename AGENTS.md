# AGENTS.md

This file provides guidance to coding agents working with code in this repository.

DuckDB.NET is an ADO.NET provider and low-level bindings for DuckDB. The solution (`DuckDB.NET.slnx`) has five
projects: `Bindings` (P/Invoke over the DuckDB C API), `Data` (the ADO.NET provider), `Test` (xUnit),
`Samples` and `Benchmarks` (BenchmarkDotNet). The libraries target `net8.0;net10.0`; the test project targets
**net8.0 only**, so `dotnet test -f net10.0` silently runs nothing. The SDK is pinned by `global.json` (10.0.2xx).
User documentation lives in a separate docfx repository (`S:\src\DuckDB.NET-Docs`, published at https://duckdb.net).

## Commands

```bash
# Build everything the way CI does (Full = download native libraries for all platforms)
dotnet build --configuration Release /p:BuildType=Full

# Run all tests (CI also adds /p:CollectCoverage=true /p:CoverletOutputFormat=lcov)
dotnet test DuckDB.NET.Test/Test.csproj --configuration Release /p:BuildType=Full

# One class, one test, or everything except the slow VARINT parameter tests
dotnet test DuckDB.NET.Test/Test.csproj --filter "FullyQualifiedName~DuckDB.NET.Test.TableFunctionTests"
dotnet test DuckDB.NET.Test/Test.csproj --filter "FullyQualifiedName~DuckDB.NET.Test.TableFunctionTests.TestTableFunction"
dotnet test DuckDB.NET.Test/Test.csproj --filter "FullyQualifiedName!~VarintTest"

# Packages: the plain packages need a separately installed native library; /p:BuildType=Full produces the .Full variants
dotnet pack DuckDB.NET.Data/Data.csproj --configuration Release /p:BuildType=Full

# Benchmarks
dotnet run -c Release --project DuckDB.NET.Benchmarks
```

### Native libraries

- The `DownloadNativeLibs` target in `Bindings.csproj` (with `DownloadNativeLibs.targets`) downloads the DuckDB
  release zips into `DuckDB.NET.Bindings/obj/runtimes/<rid>/native`. The test project always triggers this download,
  whatever `BuildType` is.
- **The cache is version-blind:** a platform is downloaded only if its folder is missing. After changing the DuckDB
  version, delete `DuckDB.NET.Bindings/obj/runtimes`, rebuild, and confirm the version with `SELECT version()`.
- **Nightly builds:** `/p:NightlyBuild=true /p:DuckDbArtifactRoot=https://artifacts.duckdb.org/latest` downloads
  DuckDB's `duckdb-shared-libs-<platform>.tar.gz` instead. CI does this on the schedule, against the
  `nightly-builds` branch.

## DuckDB version and releases

- **Current target: DuckDB v1.5.6.** The version is set in `DuckDB.NET.Bindings/Bindings.csproj`
  (`DuckDbArtifactRoot`). The "Updated to DuckDB vX" line in the `PackageReleaseNotes` of both `Bindings.csproj` and
  `Data.csproj` also needs updating. Every `LibraryImport` names its `EntryPoint`, so after an update, compare those
  names with the library's exports to catch removed functions.
- **Package version:** comes from git tags through GitVersion (`GitVersion.yml`). DuckDB.NET versions follow the
  DuckDB version (tag `1.5.6`).
- **Release steps:**
  1. Update the release notes in both project files.
  2. Merge `develop` into `main` and tag.
  3. Wait for the "Build" run on `main` to succeed.
  4. Start the `NuGet` workflow (`.github/workflows/NuGet.yml`, manual) on `main`. It publishes the
     `nugetPackages-main` artifact of the latest *successful* main build, so starting it earlier publishes an old
     build. Pushes to `develop` publish to GitHub Packages only.

## Architecture

### Bindings (`DuckDB.NET.Bindings`)

- **`NativeMethods`:** a partial class split into one file per C API area under `NativeMethods/` (Appender, Arrow,
  DataChunks, PreparedStatements, Query, ScalarFunction, StreamingResult, TableFunction, Value, Vectors, ...). Each
  declaration is a `LibraryImport` with an explicit `EntryPoint` and the `DuckDbLibrary` constant; some hot paths
  use `[SuppressGCTransition]`.
- **Lifetimes:** native objects are `SafeHandle`s in `DuckDBWrapperObjects.cs` (database, connection, prepared
  statement, data chunk, logical type, value, ...). The exception is **`DuckDBResult`**, a plain struct in
  `DuckDBNativeObjects.cs` with no finalizer. Every result must be `Close()`d exactly once, or it leaks permanently.
- **Strings:** returned strings use the marshallers in `DuckDBStringMarshallers.cs`. Use
  `DuckDBOwnedStringMarshaller` when DuckDB owns the memory, and `DuckDBCallerOwnedStringMarshaller` when the caller
  must `duckdb_free` it.

### Connections (`DuckDB.NET.Data/Connection`)

- **Pooling:** `DuckDBConnection` gets its native connection from the static `ConnectionManager`. It caches one
  `FileReference` (native database plus a reference count) per file. Returning the last connection disposes the
  database and removes the cache entry.
- **Data sources:**
  - `:memory:` gives every connection its own private database;
  - `:memory:?cache=shared` shares one;
  - `md:<db>?motherduck_token=...` connects to MotherDuck.
- **Configuration:** `DuckDBConnectionStringBuilder` normalizes `Data Source`/`DataSource` and exposes DuckDB
  configuration options as typed properties.
- **Why not DuckDB's own instance cache:** `duckdb_get_or_create_from_cache` fails under concurrent opens of the same
  path (duckdb/duckdb#22277), so `ConnectionManager` does the pooling itself.

### Commands and results

- **Execution:** `DuckDBCommand` calls `PreparedStatement.PrepareMultiple`, an iterator that extracts the statements
  and prepares and executes one statement per `MoveNext()`. Parameters are converted by `DuckDBTypeMap` and
  `ClrToDuckDBConverter` (positional and named).
- **Result modes:** `UseStreamingMode` **defaults to `false`**, so results are materialized. `true` uses
  `duckdb_execute_prepared_streaming` and `duckdb_stream_fetch_chunk`.
- **Errors:** every native result error goes through `DuckDBResultExtensions.ThrowOnError`. An interrupt becomes
  `OperationCanceledException`. An exception thrown by a C# UDF is attached as `InnerException` through
  `UdfExceptionStore`, which is keyed by connection ID. Connection IDs repeat across databases, so parallel tests on
  different databases can see each other's stored exception. This is a known flaky test source.
- **`DuckDBDataReader`:** reads one chunk at a time and creates readers through `VectorDataReaderFactory`, one per
  column. Composite readers (struct, list, map, decimal) hold child readers. After the first chunk, readers are
  reused through `VectorDataReaderBase.Reset(IntPtr vector)`. A new `VectorDataReaderBase` subclass must be
  registered in the factory, and must override `Reset` if it has child readers.
- **Invariants in `DuckDBDataReader`, each guarded by a regression test:**
  - A NULL chunk from a streaming fetch means either the end of the results or an error. Check the result's error
    before treating it as the end.
  - Reset the row counters *before* fetching, so a failed fetch cannot leave accessors reading the freed chunk.
  - `NextResult()` closes the previous result: its chunk first, then the result.
  - `Close()` releases the reader's own objects and statements *before* `CommandBehavior.CloseConnection` closes the
    connection. Otherwise a still-alive prepared statement keeps the native database alive after `ConnectionManager`
    disposes it, and the next open creates a second instance on the same file. That corrupts data on Linux and
    macOS, and fails with "file in use" on Windows.
- **Arrow:** `ExecuteArrowStream` and `ExecuteArrowBatchesAsync` stream results as Apache Arrow record batches
  (`Arrow/DuckDBArrowArrayStream.cs`), using DuckDB's Arrow C Data Interface.

### Writing data: appenders and UDF output

- **Writers:** `VectorDataWriterFactory` (`DataChunk/Writer`) creates the column writers used by the appender and by
  scalar and table function output. `CollectionVectorDataWriter` holds the shared collection logic;
  `ListVectorDataWriter` (running offsets, growth) and `ArrayVectorDataWriter` (fixed size, row-based positions)
  are separate because DuckDB stores the two differently.
- **`DuckDBAppender`:** the low-level appender (`connection.CreateAppender(table)`, then `CreateRow().AppendValue(...)`,
  or the allocation-free `AppendRow`). It buffers rows in a data chunk of `VectorSize` (2048) rows. It does **no type
  checking** and writes the raw bytes of the .NET type it is given; the docs call this out. A row is written only once
  every column has a value. Moving on from an incomplete row discards it and faults the appender.
- **`DuckDBMappedAppender<T, TMap>`:** the type-checked alternative (`CreateAppender<T, TMap>(table)`). A
  `DuckDBAppenderMap<T>` subclass declares the columns with `Map`, `DefaultValue` and `NullValue`, and type
  mismatches are reported when the appender is created. See `AppenderMap-Usage.md`.
- **UDFs:** scalar functions (`DuckDBConnection.ScalarFunction*.cs`) and table functions
  (`DuckDBConnection.TableFunction*.cs`) are registered through generic overloads for different parameter counts.
  Callbacks are pinned with `GCHandle`. A table function creates its data enumerator per scan init, not per bind,
  because DuckDB re-initializes scans without re-binding, for example in recursive CTEs.

## Coding style

There is no `.editorconfig`, so match the surrounding code:
- **Layout:** file-scoped namespaces (`namespace DuckDB.NET.Data;`) and 4-space indentation.
- **Naming:** `PascalCase` for types, methods and properties; `camelCase` for locals and fields, with no underscore
  prefix.
- **Language features:** `LangVersion` is `latest`. Use `var` when the type is obvious. Primary constructors and
  collection expressions (`[]`) are already used.
- **Nullable reference types:** enabled in `Bindings`, `Data` and `Benchmarks`, but **disabled in the test
  project**. Don't put `?` on reference types in tests (`string?`, `List<object?>`); it produces CS8632 warnings.
  `int?` and other nullable value types are fine.
- **Files:** files are UTF-8, and about half start with a byte-order mark. The checkout uses CRLF line endings
  (`core.autocrlf`). Edits made with scripts must keep the file's byte-order mark as it is and keep CRLF throughout:
  no stray LF, no doubled CR, and no lone CR at the end of a file. Otherwise git can treat the file as binary.

## Tests

Test classes take the per-class `DuckDBDatabaseFixture` (a shared in-memory connection) through
`DuckDBTestBase`. Test classes run in parallel, each with its own database. `connection.ExecuteNonQuery(sql)` is
an extension in `Extensions/DbConnectionExtension.cs`.

```csharp
public class MyTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void TestSomething()
    {
        Connection.ExecuteNonQuery("CREATE TABLE ...");
    }
}
```

Tests that close or reconfigure a connection, or need a file database, should create their own `DuckDBConnection`
instead of using the shared fixture connection.

Rules for every new or changed test (both have caused tests that passed locally and failed CI):
- **Match `AppendValue` argument types to the column types exactly.** A bare integer literal such as
  `AppendValue(1)` binds to a 1-byte overload, and so does `AppendValue((int)1)`, because it is still a constant.
  Written into an INTEGER column, that stores garbage. Use `(int?)1`, a typed variable, or the exact CLR type of
  the column.
- **To check row order, use `ORDER BY rowid`** instead of adding an ordering column.
- **Run a new test at least 5 times before committing.** One pass proves nothing.

**Known flaky failure:** with DuckDB 1.5.x, a later-chunk error test can fail with `OperationCanceledException`.
A worker-thread error sets the interrupt flag, and a streaming fetch can report that flag instead of the real error.
DuckDB fixes this in 2.0. Re-run the job; don't work around it.

## Known debugging issue

While debugging code that uses DuckDB.NET, `System.AccessViolationException` can appear because the debugger
interacts with native memory during marshalling. For the workaround, see
https://youtrack.jetbrains.com/issue/RIDER-114126.

Building DuckDB extensions in C# is a separate project: https://github.com/Giorgi/DuckDB.ExtensionKit

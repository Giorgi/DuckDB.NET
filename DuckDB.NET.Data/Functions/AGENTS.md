# AGENTS.md: the registration of functions

This folder registers scalar and table functions with DuckDB: the low-level registration and DuckDB's callbacks
(`ScalarFunctionRegistration`, `TableFunctionRegistration`), the typed functions (`TypedScalarFunctions`,
`TypedTableFunctions`: the loops, NULL handling, projections, parameters), and the state they keep (`*Info`, the
scans). The public methods are not here: `DuckDBConnection.ScalarFunction*.cs` and `DuckDBConnection.TableFunction*.cs`
pass the native connection on to the classes here, in one line each.

**DuckDB.ExtensionKit compiles every file of this folder**, from DuckDB.NET as a git submodule, into extensions
built with Native AOT. Its own public methods forward to the same classes. A change here reaches the extensions
when the submodule moves, and breaks their build if it breaks a rule below. The loops are part of the write path:
before changing them, read `DataChunk/Writer/DESIGN.md`.

**On `nightly-vnext` (the C API V2 migration) this folder is on V2, and ExtensionKit cannot compile it.** The code
here takes the native V2 connection (`DuckDBV2Connection`) and calls `NativeMethodsV2`, and `UdfErrors` keeps a
function's exception on that connection, found through `DuckDBV2CallbackScope`. The rules below are develop's, for
the V1 code that ExtensionKit compiles; on this branch the first two read "the native V2 connection" and "only
through `NativeMethodsV2`". ExtensionKit follows this folder again once it builds extensions on V2 as well
(docs/V2-MIGRATION.md, stages 5 and 6).

## Rules for the files of this folder

- **No ADO.NET.** Take the native connection (`DuckDBNativeConnection`), never `DuckDBConnection`, a command or a
  reader. ExtensionKit has none of them.
- **Native calls only through `NativeMethods`, and only functions of the extension C API v1.2.0.** That is the
  C API an extension built with ExtensionKit targets by default; it loads on DuckDB 1.2.0 and later. Every function
  called here today is in it. A function that is not, such as the client context functions, belongs in a file
  outside this folder.
- **The exception of a function goes through `UdfErrors`** (`Connection/UdfErrors.cs`), and nothing here calls
  `UdfExceptionStore`. DuckDB.NET keeps the exception for the command that ran the query; ExtensionKit has a
  `UdfErrors` of its own with the same methods, which does nothing. A new method of `UdfErrors` needs one in
  ExtensionKit too.
- **The public types are named, not defined, here:** `TableFunction`, `ColumnInfo`, `CardinalityHint`,
  `ProjectedColumn`, `ScalarFunctionOptions` and `NamedAttribute`. They are public in `DuckDB.NET.Data`, and
  ExtensionKit defines types of the same names in its own namespaces. Use only what both have; a new member that the
  code here uses has to be added to ExtensionKit's type.
- **A new public overload** in DuckDB.NET (`DuckDBConnection` or the two extension classes) needs its one-line
  counterpart in ExtensionKit. Nothing fails if it is missing; the overload is just not there.
- **Native AOT:** an extension is compiled with Native AOT and trimmed. Code that works by reflection or builds code
  at run time (`MakeGenericType`, `Expression.Compile`, looking up a method by name) can work here and fail in an
  extension. `ParseProjection` keeps the operators of `decimal` for that reason. DuckDB.NET's tests do not run with
  Native AOT; ExtensionKit's do.

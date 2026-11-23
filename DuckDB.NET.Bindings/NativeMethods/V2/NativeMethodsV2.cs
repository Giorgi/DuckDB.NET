using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Native;

/// <summary>
/// P/Invoke declarations for the DuckDB C API V2 (<c>duckdb_v2.h</c>), grouped like <see cref="NativeMethods"/>.
/// </summary>
/// <remarks>
/// The V2 API is still marked unstable by DuckDB, so these declarations follow the signatures of the DuckDB version
/// this package is built against and can change with it. The conventions of V2 apply to every declaration:
/// <list type="bullet">
/// <item>Every function returns a <see cref="DuckDBErrorCode"/>; <see cref="DuckDBErrorCode.None"/> is the only
/// success. A function that can fail takes the error info slot as its last parameter: pass a variable holding
/// <see cref="IntPtr.Zero"/>, and after a failure read it and destroy it with
/// <see cref="ErrorInfo.DuckDBV2ErrorInfoDestroy"/>. A successful call leaves the slot untouched.</item>
/// <item>Strings go in as a pointer to UTF-8 bytes and a length, and need no null terminator. Strings come out as a
/// borrowed pointer and a length, owned by the handle they were read from; read them with
/// <see cref="DuckDBV2Utf8.ToManaged"/> and never free them.</item>
/// <item>A destructor takes the handle by reference and sets it to null.</item>
/// </list>
/// </remarks>
[SuppressMessage("Design", "S4200", Justification = "The P/Invoke declarations are the public API of the bindings package.")]
public static partial class NativeMethodsV2
{
    private const string DuckDbLibrary = "duckdb";
}

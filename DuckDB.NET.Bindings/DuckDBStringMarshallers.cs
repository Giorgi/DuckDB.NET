namespace DuckDB.NET.Native;

/// <summary>
/// Marshaller for DuckDB-owned strings that must not be freed by the caller.
/// Used for error messages, column names, version strings, and config flags.
/// </summary>
[CustomMarshaller(typeof(string), MarshalMode.ManagedToUnmanagedOut, typeof(DuckDBOwnedStringMarshaller))]
public static class DuckDBOwnedStringMarshaller
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe string? ConvertToManaged(byte* unmanaged) => Utf8StringMarshaller.ConvertToManaged(unmanaged);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void Free(byte* unmanaged) { }
}

/// <summary>
/// Marshaller for caller-owned strings that must be freed with duckdb_free.
/// Used for duckdb_get_varchar, duckdb_enum_dictionary_value, duckdb_struct_type_child_name, and duckdb_open_ext error.
/// </summary>
[CustomMarshaller(typeof(string), MarshalMode.ManagedToUnmanagedOut, typeof(DuckDBCallerOwnedStringMarshaller))]
public static class DuckDBCallerOwnedStringMarshaller
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe string? ConvertToManaged(byte* unmanaged) => Utf8StringMarshaller.ConvertToManaged(unmanaged);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void Free(byte* unmanaged) => NativeMethods.Helpers.DuckDBFree((IntPtr)unmanaged);
}

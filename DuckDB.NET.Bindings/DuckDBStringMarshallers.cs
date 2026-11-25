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

namespace DuckDB.NET.Native;

public partial class NativeMethodsV2
{
    // Environment, instance and connection: an environment holds instances, an instance holds attached databases and
    // a connection runs statements on an instance.
    public static unsafe partial class Startup
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_environment_create")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2EnvironmentCreate(out DuckDBV2Environment environment, ref IntPtr error);

        // Refuses with ResourceInUse while an instance created through the environment is alive.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_environment_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2EnvironmentDestroy(ref IntPtr environment);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_environment_get_instance_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2EnvironmentGetInstanceCount(DuckDBV2Environment environment, out ulong count, ref IntPtr error);

        // An instance starts with no database attached.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_instance_create")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2InstanceCreate(DuckDBV2Environment environment, out DuckDBV2Instance instance, ref IntPtr error);

        // The options are startup settings given as text, as SQL's SET takes them: optionCount names and values, each
        // a pointer to UTF-8 bytes and a length.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_instance_create_with_options")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2InstanceCreateWithOptions(DuckDBV2Environment environment,
                                                                                 byte** optionNames, ulong* optionNameLengths,
                                                                                 byte** optionValues, ulong* optionValueLengths,
                                                                                 ulong optionCount,
                                                                                 out DuckDBV2Instance instance, ref IntPtr error);

        // Always succeeds. The instance lives on while a connection still references it.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_instance_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2InstanceDestroy(ref IntPtr instance);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_connection_create")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ConnectionCreate(DuckDBV2Instance instance, out DuckDBV2Connection connection, ref IntPtr error);

        // Always succeeds, and releases the connection's reference on its instance.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_connection_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ConnectionDestroy(ref IntPtr connection);

        // Borrowed from the library: never freed.
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_library_version")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2LibraryVersion(out byte* version, out ulong versionLength, ref IntPtr error);
    }

    public static partial class Query
    {
        // Destroying a result before its end aborts the statement.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_result_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ResultDestroy(ref IntPtr result);
    }
}

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class ScalarFunction
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBCreateScalarFunction();

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyScalarFunction(ref IntPtr scalarFunction);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_name", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetName(IntPtr scalarFunction, string name);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_varargs")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_volatile")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_add_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_return_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_register_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBRegisterScalarFunction(DuckDBNativeConnection con, IntPtr scalarFunction);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_error", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetError(IntPtr info, string error);
    }
}

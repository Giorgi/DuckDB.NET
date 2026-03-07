namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class ScalarFunction
    {
        // Maybe [SuppressGCTransition]: new ScalarFunction — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBCreateScalarFunction();

        // Maybe [SuppressGCTransition]: delete ScalarFunction — one small deallocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyScalarFunction(ref IntPtr scalarFunction);

        // Maybe [SuppressGCTransition]: strdup of name string — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_name", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetName(IntPtr scalarFunction, string name);

        // Maybe [SuppressGCTransition]: copies LogicalType — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_varargs")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_special_handling")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetSpecialHandling(IntPtr scalarFunction);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_volatile")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction);

        // Maybe [SuppressGCTransition]: copies LogicalType + vector push — small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_add_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type);

        // Maybe [SuppressGCTransition]: copies LogicalType — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_return_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_bind")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetBind(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_register_scalar_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBRegisterScalarFunction(DuckDBNativeConnection con, IntPtr scalarFunction);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_bind_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBScalarFunctionBindGetExtraInfo(IntPtr info);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_get_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBScalarFunctionGetBindData(IntPtr info);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_get_client_context")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionGetClientContext(IntPtr info, out DuckDBClientContext outContext);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_scalar_function_set_error", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBScalarFunctionSetError(IntPtr info, string error);
    }
}

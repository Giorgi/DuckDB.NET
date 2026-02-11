namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class TableFunction
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBCreateTableFunction();

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyTableFunction(ref IntPtr tableFunction);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_name")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBTableFunctionSetName(IntPtr tableFunction, SafeUnmanagedMemoryHandle name);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_add_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_bind")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_init")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> callback);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_register_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBRegisterTableFunction(DuckDBNativeConnection con, IntPtr tableFunction);

        #region TableFunctionBind

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBBindGetExtraInfo(IntPtr info);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_add_result_column")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBBindAddResultColumn(IntPtr info, SafeUnmanagedMemoryHandle name, DuckDBLogicalType type);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_parameter_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBBindGetParameterCount(IntPtr info);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBBindGetParameter(IntPtr info, ulong index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_set_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_set_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBBindSetError(IntPtr info, SafeUnmanagedMemoryHandle error);

        #endregion

        #region TableFunction

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBFunctionGetExtraInfo(IntPtr info);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_get_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBFunctionGetBindData(IntPtr info);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_set_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBFunctionSetError(IntPtr info, SafeUnmanagedMemoryHandle error);

        #endregion
    }
}

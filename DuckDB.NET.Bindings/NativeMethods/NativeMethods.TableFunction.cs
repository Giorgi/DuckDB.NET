namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class TableFunction
    {
        // Maybe [SuppressGCTransition]: new TableFunction — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBCreateTableFunction();

        // Maybe [SuppressGCTransition]: delete TableFunction — one small deallocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyTableFunction(ref IntPtr tableFunction);

        // Maybe [SuppressGCTransition]: strdup of name string — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_name", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBTableFunctionSetName(IntPtr tableFunction, string name);

        // Maybe [SuppressGCTransition]: copies LogicalType + vector push — small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_add_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_bind")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_init")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_table_function_set_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> callback);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_register_table_function")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBRegisterTableFunction(DuckDBNativeConnection con, IntPtr tableFunction);

        #region TableFunctionBind

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBBindGetExtraInfo(IntPtr info);

        // Maybe [SuppressGCTransition]: strdup name + copies LogicalType — two small allocations
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_add_result_column", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBBindAddResultColumn(IntPtr info, string name, DuckDBLogicalType type);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_parameter_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBBindGetParameterCount(IntPtr info);

        // Maybe [SuppressGCTransition]: new Value copy — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_get_parameter")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBBindGetParameter(IntPtr info, ulong index);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_set_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        // Maybe [SuppressGCTransition]: strdup error string — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_set_error", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBBindSetError(IntPtr info, string error);

        #endregion

        #region TableFunction

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_get_extra_info")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBFunctionGetExtraInfo(IntPtr info);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_get_bind_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBFunctionGetBindData(IntPtr info);

        // Maybe [SuppressGCTransition]: strdup error string — one small allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_function_set_error", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBFunctionSetError(IntPtr info, string error);

        #endregion
    }
}

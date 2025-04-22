using System.Runtime.InteropServices;
using System;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class TableFunction
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_table_function")]
        public static extern IntPtr DuckDBCreateTableFunction();

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_table_function")]
        public static extern void DuckDBDestroyTableFunction(out IntPtr tableFunction);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_set_name")]
        public static extern void DuckDBTableFunctionSetName(IntPtr tableFunction, SafeUnmanagedMemoryHandle name);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_add_parameter")]
        public static extern void DuckDBTableFunctionAddParameter(IntPtr tableFunction, DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_set_extra_info")]
        public static extern unsafe void DuckDBTableFunctionSetExtraInfo(IntPtr tableFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_set_bind")]
        public static extern unsafe void DuckDBTableFunctionSetBind(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> bind);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_set_init")]
        public static extern unsafe void DuckDBTableFunctionSetInit(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, void> init);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_table_function_set_function")]
        public static extern unsafe void DuckDBTableFunctionSetFunction(IntPtr tableFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, void> callback);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_register_table_function")]
        public static extern DuckDBState DuckDBRegisterTableFunction(DuckDBNativeConnection con, IntPtr tableFunction);

        #region TableFunctionBind

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_get_extra_info")]
        public static extern IntPtr DuckDBBindGetExtraInfo(IntPtr info);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_add_result_column")]
        public static extern void DuckDBBindAddResultColumn(IntPtr info, SafeUnmanagedMemoryHandle name, DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_get_parameter_count")]
        public static extern ulong DuckDBBindGetParameterCount(IntPtr info);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_get_parameter")]
        public static extern DuckDBValue DuckDBBindGetParameter(IntPtr info, ulong index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_set_bind_data")]
        public static extern unsafe void DuckDBBindSetBindData(IntPtr info, IntPtr bindData, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_set_error")]
        public static extern void DuckDBBindSetError(IntPtr info, SafeUnmanagedMemoryHandle error);

        #endregion

        #region TableFunction

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_function_get_extra_info")]
        public static extern IntPtr DuckDBFunctionGetExtraInfo(IntPtr info);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_function_get_bind_data")]
        public static extern IntPtr DuckDBFunctionGetBindData(IntPtr info);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_function_set_error")]
        public static extern void DuckDBFunctionSetError(IntPtr info, SafeUnmanagedMemoryHandle error);

        #endregion
    }
}
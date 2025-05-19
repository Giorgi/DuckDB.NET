using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class ScalarFunction
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_scalar_function")]
        public static extern IntPtr DuckDBCreateScalarFunction();

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_scalar_function")]
        public static extern void DuckDBDestroyScalarFunction(ref IntPtr scalarFunction);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_name")]
        public static extern void DuckDBScalarFunctionSetName(IntPtr scalarFunction, SafeUnmanagedMemoryHandle name);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_varargs")]
        public static extern void DuckDBScalarFunctionSetVarargs(IntPtr scalarFunction, DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_volatile")]
        public static extern void DuckDBScalarFunctionSetVolatile(IntPtr scalarFunction);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_add_parameter")]
        public static extern void DuckDBScalarFunctionAddParameter(IntPtr scalarFunction, DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_return_type")]
        public static extern void DuckDBScalarFunctionSetReturnType(IntPtr scalarFunction, DuckDBLogicalType type);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_extra_info")]
        public static extern unsafe void DuckDBScalarFunctionSetExtraInfo(IntPtr scalarFunction, IntPtr extraInfo, delegate* unmanaged[Cdecl]<IntPtr, void> destroy);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_set_function")]
        public static extern unsafe void DuckDBScalarFunctionSetFunction(IntPtr scalarFunction, delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_register_scalar_function")]
        public static extern DuckDBState DuckDBRegisterScalarFunction(DuckDBNativeConnection con, IntPtr scalarFunction);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_scalar_function_get_extra_info")]
        public static extern IntPtr DuckDBScalarFunctionGetExtraInfo(IntPtr scalarFunction);
    }
}
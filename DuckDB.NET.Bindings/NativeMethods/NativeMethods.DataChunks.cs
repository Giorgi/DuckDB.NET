using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class DataChunks
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_column_count")]
        public static extern long DuckDBDataChunkGetColumnCount(DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_vector")]
        public static extern IntPtr DuckDBDataChunkGetVector(DuckDBDataChunk chunk, long columnIndex);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_size")]
        public static extern long DuckDBDataChunkGetSize(DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_vector_get_column_type")]
        public static extern DuckDBLogicalType DuckDBVectorGetColumnType(IntPtr vector);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_vector_get_data")]
        public static extern unsafe void* DuckDBVectorGetData(IntPtr vector);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_vector_get_validity")]
        public static extern unsafe ulong* DuckDBVectorGetValidity(IntPtr vector);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_list_vector_get_child")]
        public static extern IntPtr DuckDBListVectorGetChild(IntPtr vector);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_list_vector_get_size")]
        public static extern long DuckDBListVectorGetSize(IntPtr vector);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_struct_vector_get_child")]
        public static extern IntPtr DuckDBStructVectorGetChild(IntPtr vector, long index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_data_chunk")]
        public static extern void DuckDBDestroyDataChunk(out IntPtr chunk);
    }
}
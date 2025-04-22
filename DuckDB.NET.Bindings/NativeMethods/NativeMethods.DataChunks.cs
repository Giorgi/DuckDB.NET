using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#data-chunk-interface
    public static class DataChunks
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_data_chunk")]
        public static extern DuckDBDataChunk DuckDBCreateDataChunk(IntPtr[] types, ulong count);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_data_chunk")]
        public static extern void DuckDBDestroyDataChunk(out IntPtr chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_reset")]
        public static extern void DuckDBDataChunkReset(DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_column_count")]
        public static extern long DuckDBDataChunkGetColumnCount(DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_vector")]
        public static extern IntPtr DuckDBDataChunkGetVector(DuckDBDataChunk chunk, long columnIndex);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_get_size")]
        public static extern ulong DuckDBDataChunkGetSize(DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_data_chunk_set_size")]
        public static extern long DuckDBDataChunkSetSize(DuckDBDataChunk chunk, ulong size);
    }
}
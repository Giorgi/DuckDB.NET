using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#extract-statements
    public static class ExtractStatements
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_extract_statements")]
        public static extern int DuckDBExtractStatements(DuckDBNativeConnection connection, string query, out DuckDBExtractedStatements extractedStatements);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_extract_statements")]
        public static extern int DuckDBExtractStatements(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, out DuckDBExtractedStatements extractedStatements);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare_extracted_statement")]
        public static extern DuckDBState DuckDBPrepareExtractedStatement(DuckDBNativeConnection connection, DuckDBExtractedStatements extractedStatements, long index, out DuckDBPreparedStatement preparedStatement);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_extract_statements_error")]
        public static extern IntPtr DuckDBExtractStatementsError(DuckDBExtractedStatements extractedStatements);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_extracted")]
        public static extern void DuckDBDestroyExtracted(out IntPtr extractedStatements);
    }
}
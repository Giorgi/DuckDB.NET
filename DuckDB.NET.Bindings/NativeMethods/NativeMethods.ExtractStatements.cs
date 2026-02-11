namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#extract-statements
    public static partial class ExtractStatements
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_extract_statements", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial int DuckDBExtractStatements(DuckDBNativeConnection connection, string query, out DuckDBExtractedStatements extractedStatements);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepare_extracted_statement")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBPrepareExtractedStatement(DuckDBNativeConnection connection, DuckDBExtractedStatements extractedStatements, long index, out DuckDBPreparedStatement preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_extract_statements_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBExtractStatementsError(DuckDBExtractedStatements extractedStatements);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_extracted")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyExtracted(ref IntPtr extractedStatements);
    }
}

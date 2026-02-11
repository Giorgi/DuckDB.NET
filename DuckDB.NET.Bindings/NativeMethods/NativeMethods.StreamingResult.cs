namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class StreamingResult
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_stream_fetch_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDataChunk DuckDBStreamFetchChunk(DuckDBResult result);
    }
}

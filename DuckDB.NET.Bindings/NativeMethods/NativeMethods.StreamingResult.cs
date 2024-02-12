using System.Runtime.InteropServices;

namespace DuckDB.NET;

public partial class NativeMethods
{
    public static class StreamingResult
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_stream_fetch_chunk")]
        public static extern DuckDBDataChunk DuckDBStreamFetchChunk([In, Out] DuckDBResult result);
    }
}
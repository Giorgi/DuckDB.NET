namespace DuckDB.NET.Data.Internal
{
    /// <summary>
    /// Just makes it easier to pass/receive this data from the ConnectionManager
    /// </summary>
    internal class ConnectionReference
    {
        public ConnectionReference(FileRefCounter fileRefCounter, DuckDBNativeConnection nativeConnection)
        {
            FileRefCounter = fileRefCounter;
            NativeConnection = nativeConnection;
        }

        public FileRefCounter FileRefCounter { get; private set; }
        public DuckDBNativeConnection NativeConnection { get; private set; }
    }
}
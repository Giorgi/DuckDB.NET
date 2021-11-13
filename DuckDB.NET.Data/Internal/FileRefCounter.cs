namespace DuckDB.NET.Data.Internal
{
    /// <summary>
    /// Holds the connection count and DuckDBDatabase structure for a FileName
    /// </summary>
    internal class FileRefCounter
    {
        public long ConnectionCount;
        public DuckDBDatabase Database;

        public FileRefCounter(string filename)
        {
            FileName = filename;
        }

        public string FileName { get; private set; }
    }
}
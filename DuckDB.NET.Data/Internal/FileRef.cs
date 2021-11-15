using System.IO;

namespace DuckDB.NET.Data.Internal
{
    /// <summary>
    /// Holds the connection count and DuckDBDatabase structure for a FileName
    /// </summary>
    internal class FileRef
    {
        public DuckDBDatabase Database;

        private long connectionCount;

        public FileRef(string filename)
        {
            FileName = filename;
        }

        public long ConnectionCount { get; } //don't need a long, but it is slightly faster on 64 bit systems
        public string FileName { get; private set; }

        public long Decrement()
        {
            return --connectionCount;
        }

        public long Increment()
        {
            return ++connectionCount;
        }

        public override string ToString() => $"{Path.GetFileName(FileName)}";
    }
}
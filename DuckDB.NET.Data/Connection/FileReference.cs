using System.IO;

namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Holds the connection count and DuckDBDatabase structure for a FileName
/// </summary>
internal class FileReference(string filename)
{
    public DuckDBDatabase? Database { get; internal set; }

    public string FileName { get; } = filename;

    public long ConnectionCount { get; private set; } //don't need a long, but it is slightly faster on 64 bit systems

    public long Decrement()
    {
        return --ConnectionCount;
    }

    public long Increment()
    {
        return ++ConnectionCount;
    }

    public override string ToString() => $"{Path.GetFileName(FileName)}";
}
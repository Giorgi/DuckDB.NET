using DuckDB.NET.Native;
using System;

namespace DuckDB.NET.Data.DataChunk.Reader;
public interface IDuckDBDataReader
{
    Type ClrType { get; }
    DuckDBType DuckDBType { get; }

    bool IsValid(ulong offset);

    T GetValue<T>(ulong offset);
    object GetValue(ulong offset);
}
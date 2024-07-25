using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Reader;

public interface IDuckDBDataReader
{
    Type ClrType { get; }
    DuckDBType DuckDBType { get; }

    bool IsValid(ulong offset);
    
    T GetValue<T>(ulong offset);
    object GetValue(ulong offset);
}
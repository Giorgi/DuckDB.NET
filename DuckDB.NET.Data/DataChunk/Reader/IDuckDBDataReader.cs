using DuckDB.NET.Native;
using System;
using System.Diagnostics.CodeAnalysis;

#if NET8_0_OR_GREATER

namespace DuckDB.NET.Data.DataChunk.Reader;

[Experimental("DuckDBNET001")]
public interface IDuckDBDataReader
{
    Type ClrType { get; }
    DuckDBType DuckDBType { get; }

    bool IsValid(ulong offset);

    T GetValue<T>(ulong offset);
    object GetValue(ulong offset);
} 
#endif
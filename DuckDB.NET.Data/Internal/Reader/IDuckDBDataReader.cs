using System;
using System.Diagnostics.CodeAnalysis;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Reader;

#if NET8_0_OR_GREATER
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
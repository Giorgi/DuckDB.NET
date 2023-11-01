using System;

namespace DuckDB.NET.Data.Internal.Reader;

internal interface IVectorDataReader : IDisposable
{
    Type ClrType { get; }
    bool IsValid(ulong offset);
    T GetValue<T>(ulong offset);
    object GetValue(ulong offset, Type? targetType = null);
}
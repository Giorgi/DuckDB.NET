using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    public interface ITypeHandler : IDisposable
    {
        T? GetValue<T>(ulong offset);
        object GetValue(ulong offset);
        bool IsValid(ulong offset);
        Type ClrType { get; }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal interface IStructMapper
    {
        object Map(ulong offset);
    }

    internal interface IStructMapper<T> : IStructMapper
    {
        new T Map(ulong offset);
    }
}

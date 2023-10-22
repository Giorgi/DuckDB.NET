using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    public interface ITypeHandlerFactory
    {
        unsafe ITypeHandler Instantiate(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType);
    }
}

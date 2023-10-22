using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal interface IReadDecimalTypeHandler
    {
        decimal GetDecimal(ulong offset);
    }
}

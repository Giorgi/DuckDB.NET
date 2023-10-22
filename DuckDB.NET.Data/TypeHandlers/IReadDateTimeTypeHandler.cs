using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    public interface IReadDateTimeTypeHandler
    {
        DateTime GetDateTime(ulong offset);
    }
}

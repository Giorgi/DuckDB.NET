using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal interface IStreamTypeHandler
    {
        Stream GetStream(ulong offset);
    }
}

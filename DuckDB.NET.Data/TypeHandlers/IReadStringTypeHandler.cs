﻿using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal interface IReadStringTypeHandler
    {
        string GetString(ulong offset);
    }
}

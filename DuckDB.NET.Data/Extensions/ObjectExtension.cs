using System;

namespace DuckDB.NET.Data.Extensions;

internal static class NullExtension
{
    public static bool IsNull(this object? value)
        => value is null or DBNull;
}
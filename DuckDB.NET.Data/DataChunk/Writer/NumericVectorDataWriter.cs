using System;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class NumericVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendNumeric<T>(T value, ulong rowIndex)
    {
        if (!TypeExtensions.IsCompatibleWithDuckDBType<T>(columnType))
        {
            throw new InvalidOperationException($"Cannot append {typeof(T).Name} value to {columnType} column. Data types must match exactly.");
        }
        
        return AppendValueInternal(value, rowIndex);
    }

    internal override bool AppendBigInteger(BigInteger value, ulong rowIndex) => AppendValueInternal<DuckDBHugeInt>(new DuckDBHugeInt(value), rowIndex);
}

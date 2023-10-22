using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data.TypeHandlers
{
    internal class TypeHandlerFactory : ITypeHandlerFactory
    {
        public unsafe ITypeHandler Instantiate(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
        {
            return columnType switch
            {
                DuckDBType.Date => new DateTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Time => new TimeTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Interval => new IntervalTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Timestamp => new TimestampTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Varchar => new VarcharTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Boolean => new NumericTypeHandler<bool>(vector, dataPointer, validityMaskPointer),
                DuckDBType.TinyInt => new NumericTypeHandler<sbyte>(vector, dataPointer, validityMaskPointer),
                DuckDBType.SmallInt => new NumericTypeHandler<short>(vector, dataPointer, validityMaskPointer),
                DuckDBType.Integer => new NumericTypeHandler<int>(vector, dataPointer, validityMaskPointer),
                DuckDBType.BigInt => new NumericTypeHandler<long>(vector, dataPointer, validityMaskPointer),
                DuckDBType.HugeInt => new HugeIntTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedTinyInt => new NumericTypeHandler<byte>(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedSmallInt => new NumericTypeHandler<ushort>(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedInteger => new NumericTypeHandler<uint>(vector, dataPointer, validityMaskPointer),
                DuckDBType.UnsignedBigInt => new NumericTypeHandler<ulong>(vector, dataPointer, validityMaskPointer),
                DuckDBType.Float => new NumericTypeHandler<float>(vector, dataPointer, validityMaskPointer),
                DuckDBType.Double => new NumericTypeHandler<double>(vector, dataPointer, validityMaskPointer),
                DuckDBType.Decimal => new DecimalTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.Enum => new EnumTypeHandler(vector, dataPointer, validityMaskPointer),
                DuckDBType.List => new ListTypeHandler(vector, dataPointer, validityMaskPointer, this),
                _ => throw new DuckDBException("Invalid type"),
            };
        }
    }
}

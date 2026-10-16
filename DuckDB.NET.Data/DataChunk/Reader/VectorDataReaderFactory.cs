namespace DuckDB.NET.Data.DataChunk.Reader;

internal static class VectorDataReaderFactory
{
    public static unsafe VectorDataReaderBase CreateReader(IntPtr vector, DuckDBLogicalType logicalColumnType, string columnName = "")
    {
        var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalColumnType);
        var dataPointer = NativeMethods.Vectors.DuckDBVectorGetData(vector);
        var validityMaskPointer = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);

        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Date => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Time => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimeTz => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Interval => new IntervalVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Timestamp => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),

            DuckDBType.Boolean => new BooleanVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),

            DuckDBType.Map => new MapVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.List => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.Array => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, logicalColumnType, columnName),
            DuckDBType.Blob => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Varchar => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Bit => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Enum => new EnumVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Struct => new StructVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Decimal => new DecimalVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampS => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampMs => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampNs => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampTz => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TinyInt => new NumericVectorDataReader<sbyte>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.SmallInt => new NumericVectorDataReader<short>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Integer => new NumericVectorDataReader<int>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.BigInt => new NumericVectorDataReader<long>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.UnsignedTinyInt => new NumericVectorDataReader<byte>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.UnsignedSmallInt => new NumericVectorDataReader<ushort>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.UnsignedInteger => new NumericVectorDataReader<uint>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.UnsignedBigInt => new NumericVectorDataReader<ulong>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Float => new NumericVectorDataReader<float>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Double => new NumericVectorDataReader<double>(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.HugeInt => new BigIntegerVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.UnsignedHugeInt => new BigIntegerVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.VarInt => new BigIntegerVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            // A type with no reader: reading its values or asking for its .NET type throws.
            _ => new VectorDataReaderBase(dataPointer, validityMaskPointer, columnType, columnName)
        };
    }
}
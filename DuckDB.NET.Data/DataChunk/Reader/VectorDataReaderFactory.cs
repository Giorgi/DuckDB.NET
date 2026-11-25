using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal static class VectorDataReaderFactory
{
    /// <summary>
    /// A reader for a column of a V2 result, from the column's logical type. It points at no data until
    /// <see cref="VectorDataReaderBase.Reset(_duckdb_v2_vector*)"/> gives it a chunk's vector, so a result with no rows
    /// still has its columns.
    /// </summary>
    /// <remarks>
    /// A nested type's reader makes the readers of its children here too, from the type's parameters.
    /// </remarks>
    public static unsafe VectorDataReaderBase CreateReader(_duckdb_v2_logical_type* logicalType, string columnName)
    {
        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_LOGICAL_TYPE_ID typeId;
        NativeMethodsV2.duckdb_v2_logical_type_get_id(logicalType, &typeId, &error).ThrowIfFailed(ref error);

        var columnType = typeId.ToDuckDBType();

        return columnType switch
        {
            DuckDBType.Boolean => new BooleanVectorDataReader(null, null, columnType, columnName),

            DuckDBType.Blob => new StringVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Varchar => new StringVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Bit => new StringVectorDataReader(null, null, columnType, columnName),

            DuckDBType.TinyInt => new NumericVectorDataReader<sbyte>(null, null, columnType, columnName),
            DuckDBType.SmallInt => new NumericVectorDataReader<short>(null, null, columnType, columnName),
            DuckDBType.Integer => new NumericVectorDataReader<int>(null, null, columnType, columnName),
            DuckDBType.BigInt => new NumericVectorDataReader<long>(null, null, columnType, columnName),
            DuckDBType.UnsignedTinyInt => new NumericVectorDataReader<byte>(null, null, columnType, columnName),
            DuckDBType.UnsignedSmallInt => new NumericVectorDataReader<ushort>(null, null, columnType, columnName),
            DuckDBType.UnsignedInteger => new NumericVectorDataReader<uint>(null, null, columnType, columnName),
            DuckDBType.UnsignedBigInt => new NumericVectorDataReader<ulong>(null, null, columnType, columnName),
            DuckDBType.Float => new NumericVectorDataReader<float>(null, null, columnType, columnName),
            DuckDBType.Double => new NumericVectorDataReader<double>(null, null, columnType, columnName),

            DuckDBType.Uuid => new GuidVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Date => new DateVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Time => new TimeVectorDataReader<DuckDBTime>(null, null, columnType, columnName),
            DuckDBType.TimeNs => new TimeVectorDataReader<DuckDBTimeNs>(null, null, columnType, columnName),
            DuckDBType.TimeTz => new TimeTzVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Interval => new IntervalVectorDataReader(null, null, columnType, columnName),
            DuckDBType.Timestamp => new TimestampVectorDataReader(null, null, columnType, columnName),
            DuckDBType.TimestampS => new TimestampVectorDataReader(null, null, columnType, columnName),
            DuckDBType.TimestampMs => new TimestampVectorDataReader(null, null, columnType, columnName),
            DuckDBType.TimestampNs => new TimestampVectorDataReader(null, null, columnType, columnName),
            DuckDBType.TimestampTz => new TimestampVectorDataReader(null, null, columnType, columnName),

            DuckDBType.HugeInt => new BigIntegerVectorDataReader(null, null, columnType, columnName),
            DuckDBType.UnsignedHugeInt => new BigIntegerVectorDataReader(null, null, columnType, columnName),
            DuckDBType.VarInt => new BigIntegerVectorDataReader(null, null, columnType, columnName),

            DuckDBType.Decimal => new DecimalVectorDataReader((byte)DuckDBV2LogicalTypes.GetUIntParam(logicalType, 0),
                                                              (byte)DuckDBV2LogicalTypes.GetUIntParam(logicalType, 1), columnName),
            DuckDBType.Enum => new EnumVectorDataReader(logicalType, columnName),

            DuckDBType.List => new ListVectorDataReader(logicalType, columnType, columnName),
            DuckDBType.Array => new ListVectorDataReader(logicalType, columnType, columnName),
            DuckDBType.Map => new MapVectorDataReader(logicalType, columnName),
            DuckDBType.Struct => new StructVectorDataReader(logicalType, columnName),

            // A type with no reader, as with V1: reading its values or asking for its .NET type throws. UNION is one, and
            // so is a column that is NULL in every row (SELECT NULL), which has no values to read.
            _ => new VectorDataReaderBase(null, null, columnType, columnName)
        };
    }

}

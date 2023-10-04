using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Text;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private const int InlineStringMaxLength = 12;
    private readonly DuckDbCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBResult currentResult;
    private DuckDBDataChunk currentChunk;
    private readonly List<DuckDBResult> queryResults;

    private bool closed;
    private long rowCount;
    private int currentRow;
    private int currentResultIndex;

    private int fieldCount;
    private int recordsAffected = -1;

    private IntPtr[] vectors;
    private unsafe void*[] vectorData;
    private unsafe ulong*[] vectorValidityMask;

    private long chunkCount;
    private int currentChunkIndex;
    private int rowsReadFromCurrentChunk;
    private long currentChunkRowCount;

    internal DuckDBDataReader(DuckDbCommand command, List<DuckDBResult> queryResults, CommandBehavior behavior)
    {
        this.command = command;
        this.behavior = behavior;
        this.queryResults = queryResults;

        currentResult = queryResults[0];
        InitReaderData();
    }

    private void InitReaderData()
    {
        currentRow = -1;

        rowCount = NativeMethods.Query.DuckDBRowCount(ref currentResult);
        fieldCount = (int)NativeMethods.Query.DuckDBColumnCount(ref currentResult);
        chunkCount = NativeMethods.Types.DuckDBResultChunkCount(currentResult);

        currentChunkIndex = 0;
        rowsReadFromCurrentChunk = 0;

        InitChunkData();

        //recordsAffected = (int)NativeMethods.Query.DuckDBRowsChanged(currentResult);
    }

    private void InitChunkData()
    {
        unsafe
        {
            currentChunk?.Dispose();
            currentChunk = NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);
            currentChunkRowCount = NativeMethods.DataChunks.DuckDBDataChunkGetSize(currentChunk);

            vectors = new IntPtr[fieldCount];
            vectorData = new void*[fieldCount];
            vectorValidityMask = new ulong*[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                vectors[i] = NativeMethods.DataChunks.DuckDBDataChunkGetVector(currentChunk, i);

                vectorData[i] = NativeMethods.DataChunks.DuckDBVectorGetData(vectors[i]);
                vectorValidityMask[i] = NativeMethods.DataChunks.DuckDBVectorGetValidity(vectors[i]);
            }
        }
    }

    private unsafe T GetFieldData<T>(int ordinal) where T : unmanaged => GetFieldData<T>(vectorData[ordinal], (ulong)(rowsReadFromCurrentChunk - 1));

    private static unsafe T GetFieldData<T>(void* pointer, ulong offset) where T : unmanaged
    {
        var data = (T*)pointer + offset;
        return *data;
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetByte(ordinal) != 0;
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldData<byte>(ordinal);
    }

    private sbyte GetSByte(int ordinal)
    {
        return GetFieldData<sbyte>(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotSupportedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal).ToString();
    }

    public override unsafe DateTime GetDateTime(int ordinal)
    {
        return GetDateTime(vectorData[ordinal], (ulong)(rowsReadFromCurrentChunk - 1));
    }

    private static unsafe DateTime GetDateTime(void* pointer, ulong offset)
    {
        var data = (DuckDBTimestampStruct*)pointer + offset;
        return data->ToDateTime();
    }

    private DuckDBDateOnly GetDateOnly(int ordinal)
    {
        var date = GetFieldData<DuckDBDate>(ordinal);
        return NativeMethods.DateTime.DuckDBFromDate(date);
    }

    private DuckDBTimeOnly GetTimeOnly(int ordinal)
    {
        var time = GetFieldData<DuckDBTime>(ordinal);
        return NativeMethods.DateTime.DuckDBFromTime(time);
    }

    public override unsafe decimal GetDecimal(int ordinal)
    {
        var logicalType = NativeMethods.Query.DuckDBColumnLogicalType(ref currentResult, ordinal);

        return GetDecimal(vectorData[ordinal], (ulong)(rowsReadFromCurrentChunk - 1), logicalType);
    }

    public override double GetDouble(int ordinal)
    {
        return GetFieldData<double>(ordinal);
    }

    public override Type GetFieldType(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal) switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            DuckDBType.Boolean => typeof(bool),
            DuckDBType.TinyInt => typeof(sbyte),
            DuckDBType.SmallInt => typeof(short),
            DuckDBType.Integer => typeof(int),
            DuckDBType.BigInt => typeof(long),
            DuckDBType.UnsignedTinyInt => typeof(byte),
            DuckDBType.UnsignedSmallInt => typeof(ushort),
            DuckDBType.UnsignedInteger => typeof(uint),
            DuckDBType.UnsignedBigInt => typeof(ulong),
            DuckDBType.Float => typeof(float),
            DuckDBType.Double => typeof(double),
            DuckDBType.Timestamp => typeof(DateTime),
            DuckDBType.Interval => typeof(DuckDBInterval),
            DuckDBType.Date => typeof(DuckDBDateOnly),
            DuckDBType.Time => typeof(DuckDBTimeOnly),
            DuckDBType.HugeInt => typeof(BigInteger),
            DuckDBType.Varchar => typeof(string),
            DuckDBType.Decimal => typeof(decimal),
            DuckDBType.Blob => typeof(Stream),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
        };
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldData<float>(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        return new Guid(GetString(ordinal));
    }

    public override short GetInt16(int ordinal)
    {
        return GetFieldData<short>(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        return GetFieldData<int>(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        return GetFieldData<long>(ordinal);
    }

    private ushort GetUInt16(int ordinal)
    {
        return GetFieldData<ushort>(ordinal);
    }

    private uint GetUInt32(int ordinal)
    {
        return GetFieldData<uint>(ordinal);
    }

    private ulong GetUInt64(int ordinal)
    {
        return GetFieldData<ulong>(ordinal);
    }

    private unsafe BigInteger GetBigInteger(int ordinal)
    {
        return GetBigInteger(vectorData[ordinal], (ulong)(rowsReadFromCurrentChunk - 1));
    }

    private static unsafe BigInteger GetBigInteger(void* pointer, ulong offset)
    {
        var data = (DuckDBHugeInt*)pointer + offset;
        return data->ToBigInteger();
    }

    public override string GetName(int ordinal)
    {
        return NativeMethods.Query.DuckDBColumnName(ref currentResult, ordinal).ToManagedString(false);
    }

    public override int GetOrdinal(string name)
    {
        var columnCount = NativeMethods.Query.DuckDBColumnCount(ref currentResult);
        for (var i = 0; i < columnCount; i++)
        {
            var columnName = NativeMethods.Query.DuckDBColumnName(ref currentResult, i).ToManagedString(false);
            if (name == columnName)
            {
                return i;
            }
        }

        throw new DuckDBException($"Column with name {name} was not found.");
    }

    public override unsafe string GetString(int ordinal)
    {
        var data = (DuckDBString*)vectorData[ordinal] + rowsReadFromCurrentChunk - 1;
        return DuckDBStringToString(data);
    }

    private static unsafe string DuckDBStringToString(DuckDBString* data)
    {
        var length = *(int*)data;

        var pointer = length <= InlineStringMaxLength
            ? data->value.inlined.inlined
            : data->value.pointer.ptr;

        return new string(pointer, 0, length, Encoding.UTF8);
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        var type = NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal);

        return type switch
        {
            DuckDBType.List => (T)GetList(ordinal, typeof(T)),
            DuckDBType.Enum => GetEnum<T>(ordinal),
            _ => (T)GetValue(ordinal, type)
        };
    }

    private T GetEnum<T>(int ordinal)
    {
        var vector = vectors[ordinal];
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        var internalType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);

        long enumValue = internalType switch
        {
            DuckDBType.UnsignedTinyInt => GetByte(ordinal),
            DuckDBType.UnsignedSmallInt => GetUInt16(ordinal),
            DuckDBType.UnsignedInteger => GetUInt32(ordinal),
            _ => -1
        };

        var targetType = typeof(T);

        if (targetType == typeof(string))
        {
            var value = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, enumValue).ToManagedString();
            return (T)(object)value;
        }

        var underlyingType = Nullable.GetUnderlyingType(targetType);
        if (underlyingType != null)
        {
            if (IsDBNull(ordinal))
            {
                return default;
            }
            targetType = underlyingType;
        }

        var enumItem = (T)Enum.Parse(targetType, enumValue.ToString(CultureInfo.InvariantCulture));
        return enumItem;
    }

    public override object GetValue(int ordinal)
    {
        return IsDBNull(ordinal) ? DBNull.Value : GetValue(ordinal, NativeMethods.Query.DuckDBColumnType(ref currentResult, ordinal));
    }

    private object GetValue(int ordinal, DuckDBType columnType)
    {
        return columnType switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            DuckDBType.Boolean => GetBoolean(ordinal),
            DuckDBType.TinyInt => GetSByte(ordinal),
            DuckDBType.SmallInt => GetInt16(ordinal),
            DuckDBType.Integer => GetInt32(ordinal),
            DuckDBType.BigInt => GetInt64(ordinal),
            DuckDBType.UnsignedTinyInt => GetByte(ordinal),
            DuckDBType.UnsignedSmallInt => GetUInt16(ordinal),
            DuckDBType.UnsignedInteger => GetUInt32(ordinal),
            DuckDBType.UnsignedBigInt => GetUInt64(ordinal),
            DuckDBType.Float => GetFloat(ordinal),
            DuckDBType.Double => GetDouble(ordinal),
            DuckDBType.Timestamp => GetDateTime(ordinal),
            DuckDBType.Interval => GetDuckDBInterval(ordinal),
            DuckDBType.Date => GetDateOnly(ordinal),
            DuckDBType.Time => GetTimeOnly(ordinal),
            DuckDBType.HugeInt => GetBigInteger(ordinal),
            DuckDBType.Varchar => GetString(ordinal),
            DuckDBType.Decimal => GetDecimal(ordinal),
            DuckDBType.Blob => GetStream(ordinal),
            DuckDBType.List => GetList(ordinal),
            DuckDBType.Enum => GetEnum<string>(ordinal),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {ordinal + 1}")
        };
    }

    private object GetList(int ordinal, Type returnType = null)
    {
        var vector = vectors[ordinal];
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
        var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

        var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);
        var genericArgument = returnType?.GetGenericArguments()[0];
        var allowNulls = returnType != null && genericArgument.IsValueType && Nullable.GetUnderlyingType(genericArgument) != null;

        return type switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            DuckDBType.Boolean => allowNulls ? BuildList<bool?>() : BuildList<bool>(),
            DuckDBType.TinyInt => allowNulls ? BuildList<sbyte?>() : BuildList<sbyte>(),
            DuckDBType.SmallInt => allowNulls ? BuildList<short?>() : BuildList<short>(),
            DuckDBType.Integer => allowNulls ? BuildList<int?>() : BuildList<int>(),
            DuckDBType.BigInt => allowNulls ? BuildList<long?>() : BuildList<long>(),
            DuckDBType.UnsignedTinyInt => allowNulls ? BuildList<byte?>() : BuildList<byte>(),
            DuckDBType.UnsignedSmallInt => allowNulls ? BuildList<ushort?>() : BuildList<ushort>(),
            DuckDBType.UnsignedInteger => allowNulls ? BuildList<uint?>() : BuildList<uint>(),
            DuckDBType.UnsignedBigInt => allowNulls ? BuildList<ulong?>() : BuildList<ulong>(),
            DuckDBType.Float => allowNulls ? BuildList<float?>() : BuildList<float>(),
            DuckDBType.Double => allowNulls ? BuildList<double?>() : BuildList<double>(),
            DuckDBType.Timestamp => allowNulls ? BuildList<DateTime?>() : BuildList<DateTime>(),
#if NET6_0_OR_GREATER
            DuckDBType.Date => allowNulls
                ? genericArgument == null || genericArgument == typeof(DateTime)
                    ? BuildList<DateTime?>()
                    : BuildList<DateOnly?>()
                : genericArgument == null || genericArgument == typeof(DateTime)
                    ? BuildList<DateTime>()
                    : BuildList<DateOnly>(),
#else
            DuckDBType.Date => allowNulls ? BuildList<DateTime?>() : BuildList<DateTime>(),
#endif
#if NET6_0_OR_GREATER
            DuckDBType.Time => allowNulls
                ? genericArgument == null || genericArgument == typeof(DateTime)
                    ? BuildList<DateTime?>()
                    : BuildList<TimeOnly?>()
                : genericArgument == null || genericArgument == typeof(DateTime)
                    ? BuildList<DateTime>()
                    : BuildList<TimeOnly>(),
#else
            DuckDBType.Time => allowNulls ? BuildList<DateTime?>() : BuildList<DateTime>(),
#endif

            DuckDBType.Interval => allowNulls ? BuildList<DuckDBInterval?>() : BuildList<DuckDBInterval>(),
            DuckDBType.HugeInt => allowNulls ? BuildList<BigInteger?>() : BuildList<BigInteger>(),
            DuckDBType.Varchar => BuildList<string>(),
            DuckDBType.Decimal => allowNulls ? BuildList<decimal?>() : BuildList<decimal>(),
            _ => throw new NotImplementedException()
        };

        unsafe List<T> BuildList<T>()
        {
            var list = new List<T>();
            var listData = (DuckDBListEntry*)vectorData[ordinal] + rowsReadFromCurrentChunk - 1;

            var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);

            var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

            var targetType = typeof(T);

            if (Nullable.GetUnderlyingType(targetType) != null)
            {
                targetType = targetType.GetGenericArguments()[0];
            }

            for (ulong i = 0; i < listData->Length; i++)
            {
                var offset = i + listData->Offset;
                if (IsValid(offset, childVectorValidity))
                {
                    var item = type switch
                    {
                        DuckDBType.Varchar => DuckDBStringToString((DuckDBString*)childVectorData + offset),
                        DuckDBType.Timestamp => GetDateTime(childVectorData, offset),
                        DuckDBType.Date => GetDate(offset),
                        DuckDBType.Time => GetTime(offset),
                        DuckDBType.HugeInt => GetBigInteger(childVectorData, offset),
                        DuckDBType.Decimal => GetDecimal(childVectorData, offset, NativeMethods.DataChunks.DuckDBVectorGetColumnType(childVector)),

                        DuckDBType.Integer => GetFieldData<int>(childVectorData, offset),
                        DuckDBType.Double => GetFieldData<double>(childVectorData, offset),
                        DuckDBType.Boolean => GetFieldData<bool>(childVectorData, offset),
                        DuckDBType.TinyInt => GetFieldData<sbyte>(childVectorData, offset),
                        DuckDBType.SmallInt => GetFieldData<short>(childVectorData, offset),
                        DuckDBType.BigInt => GetFieldData<long>(childVectorData, offset),
                        DuckDBType.UnsignedTinyInt => GetFieldData<byte>(childVectorData, offset),
                        DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(childVectorData, offset),
                        DuckDBType.UnsignedInteger => GetFieldData<uint>(childVectorData, offset),
                        DuckDBType.UnsignedBigInt => GetFieldData<ulong>(childVectorData, offset),
                        DuckDBType.Float => GetFieldData<float>(childVectorData, offset),
                        DuckDBType.Interval => GetFieldData<DuckDBInterval>(childVectorData, offset),
                    };
                    list.Add((T)item);
                }
                else
                {
                    if (allowNulls)
                    {
                        list.Add((T)(object)null);
                    }
                    else
                    {
                        throw new NullReferenceException("The list contains null value");
                    }
                }
            }

            return list;

            object GetDate(ulong offset)
            {
                var dateOnly = NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(childVectorData, offset));
                if (targetType == typeof(DateTime))
                {
                    return (DateTime)dateOnly;
                }

#if NET6_0_OR_GREATER
                if (targetType == typeof(DateOnly))
                {
                    return (DateOnly)dateOnly;
                }
#endif

                return dateOnly;
            }

            object GetTime(ulong offset)
            {
                var timeOnly = NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(childVectorData, offset));
                if (targetType == typeof(DateTime))
                {
                    return (DateTime)timeOnly;
                }

#if NET6_0_OR_GREATER
                if (targetType == typeof(TimeOnly))
                {
                    return (TimeOnly)timeOnly;
                }
#endif

                return timeOnly;
            }
        }
    }

    private static unsafe decimal GetDecimal(void* data, ulong offset, DuckDBLogicalType columnType)
    {
        using (columnType)
        {
            var scale = NativeMethods.LogicalType.DuckDBDecimalScale(columnType);
            var internalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(columnType);

            var pow = (decimal)Math.Pow(10, scale);
            switch (internalType)
            {
                case DuckDBType.SmallInt:
                    return decimal.Divide(GetFieldData<short>(data, offset), pow);
                case DuckDBType.Integer:
                    return decimal.Divide(GetFieldData<int>(data, offset), pow);
                case DuckDBType.BigInt:
                    return decimal.Divide(GetFieldData<long>(data, offset), pow);
                case DuckDBType.HugeInt:
                    {
                        var hugeInt = GetBigInteger(data, offset);

                        var result = (decimal)BigInteger.DivRem(hugeInt, (BigInteger)pow, out var remainder);

                        result += decimal.Divide((decimal)remainder, pow);
                        return result;
                    }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    private DuckDBInterval GetDuckDBInterval(int ordinal)
    {
        return GetFieldData<DuckDBInterval>(ordinal);
    }

    public override int GetValues(object[] values)
    {
        for (var i = 0; i < FieldCount; i++)
        {
            values[i] = GetValue(i);
        }

        return FieldCount;
    }

    public override unsafe Stream GetStream(int ordinal)
    {
        var data = (DuckDBString*)vectorData[ordinal] + rowsReadFromCurrentChunk - 1;

        var length = *(int*)data;

        if (length <= InlineStringMaxLength)
        {
            var value = new string(data->value.inlined.inlined, 0, length, Encoding.UTF8);
            return new MemoryStream(Encoding.UTF8.GetBytes(value), false);
        }

        return new UnmanagedMemoryStream((byte*)data->value.pointer.ptr, length, length, FileAccess.Read);
    }

    public override unsafe bool IsDBNull(int ordinal)
    {
        var isValid = IsValid((ulong)rowsReadFromCurrentChunk - 1, vectorValidityMask[ordinal]);
        return !isValid;
    }

    private static unsafe bool IsValid(ulong offset, ulong* pointer)
    {
        var validityMaskEntryIndex = offset / 64;
        int validityBitIndex = (int)(offset % 64);

        var validityMaskEntryPtr = pointer + validityMaskEntryIndex;
        var validityBit = 1ul << validityBitIndex;

        var isValid = (*validityMaskEntryPtr & validityBit) != 0;
        return isValid;
    }

    public override int FieldCount => fieldCount;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => recordsAffected;

    public override bool HasRows => rowCount > 0;

    public override bool IsClosed => closed;

    public override bool NextResult()
    {
        currentResultIndex++;

        if (currentResultIndex < queryResults.Count)
        {
            currentResult = queryResults[currentResultIndex];

            InitReaderData();
            return true;
        }

        return false;
    }

    public override bool Read()
    {
        var hasMoreRows = ++currentRow < rowCount;

        if (!hasMoreRows) return false;

        if (rowsReadFromCurrentChunk == currentChunkRowCount)
        {
            currentChunkIndex++;
            rowsReadFromCurrentChunk = 0;

            InitChunkData();
        }

        rowsReadFromCurrentChunk++;

        return true;
    }

    public override int Depth { get; }

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, behavior == CommandBehavior.CloseConnection);
    }

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable
        {
            Columns =
            {
                { "ColumnOrdinal", typeof(int) },
                { "ColumnName", typeof(string) },
                { "DataType", typeof(Type) },
                { "ColumnSize", typeof(int) },
                { "AllowDBNull", typeof(bool) }
            }
        };

        var rowData = new object[5];

        for (var i = 0; i < FieldCount; i++)
        {
            rowData[0] = i;
            rowData[1] = GetName(i);
            rowData[2] = GetFieldType(i);
            rowData[3] = -1;
            rowData[4] = true;
            table.Rows.Add(rowData);
        }

        return table;
    }

    public override void Close()
    {
        if (closed) return;

        currentChunk?.Dispose();
        foreach (var result in queryResults)
        {
            result.Dispose();
        }

        if (behavior == CommandBehavior.CloseConnection)
        {
            command.CloseConnection();
        }

        closed = true;
    }
}
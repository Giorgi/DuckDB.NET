using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Text;

namespace DuckDB.NET.Data;

internal class VectorDataReader : IDisposable
{
    private const int InlineStringMaxLength = 12;

    private readonly IntPtr vector;
    private readonly unsafe void* dataPointer;
    private readonly unsafe ulong* validityMaskPointer;
    private readonly DuckDBLogicalType logicalType;
    internal DuckDBType ColumnType { get; }

    private readonly VectorDataReader listDataReader;

    private readonly byte scale;
    private readonly DuckDBType decimalType;

    internal unsafe VectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
        this.vector = vector;
        this.dataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;
        ColumnType = columnType;
        logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);

        if (ColumnType == DuckDBType.Decimal)
        {
            scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
            decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
        }

        if (ColumnType == DuckDBType.List)
        {
            using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
            var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

            var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);

            var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
            var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

            listDataReader = new VectorDataReader(childVector, childVectorData, childVectorValidity, type);
        }
    }

    internal unsafe bool IsValid(ulong offset)
    {
        var validityMaskEntryIndex = offset / 64;
        var validityBitIndex = (int)(offset % 64);

        var validityMaskEntryPtr = validityMaskPointer + validityMaskEntryIndex;
        var validityBit = 1ul << validityBitIndex;

        var isValid = (*validityMaskEntryPtr & validityBit) != 0;
        return isValid;
    }

    internal unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)dataPointer + offset);

    internal decimal GetDecimal(ulong offset)
    {
        var pow = (decimal)Math.Pow(10, scale);
        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                return decimal.Divide(GetFieldData<short>(offset), pow);
            case DuckDBType.Integer:
                return decimal.Divide(GetFieldData<int>(offset), pow);
            case DuckDBType.BigInt:
                return decimal.Divide(GetFieldData<long>(offset), pow);
            case DuckDBType.HugeInt:
                {
                    var hugeInt = GetBigInteger(offset);

                    var result = (decimal)BigInteger.DivRem(hugeInt, (BigInteger)pow, out var remainder);

                    result += decimal.Divide((decimal)remainder, pow);
                    return result;
                }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    internal unsafe DateTime GetDateTime(ulong offset)
    {
        var data = (DuckDBTimestampStruct*)dataPointer + offset;
        return data->ToDateTime();
    }

    internal unsafe string GetString(ulong offset)
    {
        var data = (DuckDBString*)dataPointer + offset;
        var length = *(int*)data;

        var pointer = length <= InlineStringMaxLength
            ? data->value.inlined.inlined
            : data->value.pointer.ptr;

        return new string(pointer, 0, length, Encoding.UTF8);
    }

    internal unsafe Stream GetStream(ulong offset)
    {
        var data = (DuckDBString*)dataPointer + offset;
        var length = *(int*)data;

        if (length <= InlineStringMaxLength)
        {
            var value = new string(data->value.inlined.inlined, 0, length, Encoding.UTF8);
            return new MemoryStream(Encoding.UTF8.GetBytes(value), false);
        }

        return new UnmanagedMemoryStream((byte*)data->value.pointer.ptr, length, length, FileAccess.Read);
    }

    internal unsafe BigInteger GetBigInteger(ulong offset)
    {
        var data = (DuckDBHugeInt*)dataPointer + offset;
        return data->ToBigInteger();
    }

    internal T GetEnum<T>(ulong offset)
    {
        var internalType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);

        long enumValue = internalType switch
        {
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
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
            if (!IsValid(offset))
            {
                return default;
            }
            targetType = underlyingType;
        }

        var enumItem = (T)Enum.Parse(targetType, enumValue.ToString(CultureInfo.InvariantCulture));
        return enumItem;
    }

    internal unsafe object GetList(ulong offset, Type returnType = null)
    {
        var listData = (DuckDBListEntry*)dataPointer + offset;

        var genericArgument = returnType?.GetGenericArguments()[0];
        var allowNulls = returnType != null && genericArgument.IsValueType && Nullable.GetUnderlyingType(genericArgument) != null;

        return listDataReader.ColumnType switch
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

        List<T> BuildList<T>()
        {
            var list = new List<T>();

            var targetType = typeof(T);

            if (Nullable.GetUnderlyingType(targetType) != null)
            {
                targetType = targetType.GetGenericArguments()[0];
            }

            for (ulong i = 0; i < listData->Length; i++)
            {
                var childOffset = i + listData->Offset;
                if (listDataReader.IsValid(childOffset))
                {
                    var item = listDataReader.GetValue(childOffset);
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

//            object GetDate(ulong offset)
//            {
//                var dateOnly = NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(childVectorData, offset));
//                if (targetType == typeof(DateTime))
//                {
//                    return (DateTime)dateOnly;
//                }

//#if NET6_0_OR_GREATER
//                if (targetType == typeof(DateOnly))
//                {
//                    return (DateOnly)dateOnly;
//                }
//#endif

//                return dateOnly;
//            }

//            object GetTime(ulong offset)
//            {
//                var timeOnly = NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(childVectorData, offset));
//                if (targetType == typeof(DateTime))
//                {
//                    return (DateTime)timeOnly;
//                }

//#if NET6_0_OR_GREATER
//                if (targetType == typeof(TimeOnly))
//                {
//                    return (TimeOnly)timeOnly;
//                }
//#endif

//                return timeOnly;
//            }
        }
    }

    internal object GetValue(ulong offset)
    {
        return ColumnType switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            DuckDBType.Boolean => GetFieldData<bool>(offset),
            DuckDBType.TinyInt => GetFieldData<sbyte>(offset),
            DuckDBType.SmallInt => GetFieldData<short>(offset),
            DuckDBType.Integer => GetFieldData<int>(offset),
            DuckDBType.BigInt => GetFieldData<long>(offset),
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
            DuckDBType.UnsignedBigInt => GetFieldData<ulong>(offset),
            DuckDBType.Float => GetFieldData<float>(offset),
            DuckDBType.Double => GetFieldData<double>(offset),
            DuckDBType.Timestamp => GetDateTime(offset),
            DuckDBType.Interval => GetFieldData<DuckDBInterval>(offset),
            DuckDBType.Date => NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(offset)),
            DuckDBType.Time => NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(offset)),
            DuckDBType.HugeInt => GetBigInteger(offset),
            DuckDBType.Varchar => GetString(offset),
            DuckDBType.Decimal => GetDecimal(offset),
            DuckDBType.Blob => GetStream(offset),
            DuckDBType.List => GetList(offset),
            DuckDBType.Enum => GetEnum<string>(offset),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type}) in column {offset + 1}")
        };
    }

    public void Dispose()
    {
        listDataReader?.Dispose();
        logicalType?.Dispose();
    }
}
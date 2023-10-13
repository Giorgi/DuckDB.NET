using System;
using System.Collections;
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

    internal Type ClrType { get; }
    internal DuckDBType DuckDBType { get; }

    private readonly VectorDataReader? listDataReader;
    private readonly Dictionary<string, VectorDataReader>? structDataReaders;

    private readonly byte scale;
    private readonly DuckDBType decimalType;
    private readonly DuckDBType enumType;

    internal unsafe VectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
        this.vector = vector;
        this.dataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;

        DuckDBType = columnType;

        logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);

        switch (DuckDBType)
        {
            case DuckDBType.Enum:
                enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
                break;
            case DuckDBType.Decimal:
                scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
                decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
                break;
            case DuckDBType.List:
                {
                    using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
                    var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

                    var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);

                    var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
                    var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

                    listDataReader = new VectorDataReader(childVector, childVectorData, childVectorValidity, type);
                    break;
                }
        }

        ClrType = DuckDBType switch
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
            DuckDBType.Enum => typeof(string),
            DuckDBType.List => typeof(List<>).MakeGenericType(listDataReader!.ClrType),
            DuckDBType.Struct => typeof(Dictionary<string, object>),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type})")
        };

        switch (DuckDBType)
        {
            case DuckDBType.Enum:
                enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
                break;
            case DuckDBType.Decimal:
                scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
                decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
                break;
            case DuckDBType.List:
                {
                    using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
                    var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

                    var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);

                    var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
                    var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

                    listDataReader = new VectorDataReader(childVector, childVectorData, childVectorValidity, type);
                    break;
                }
            case DuckDBType.Struct:
                {
                    var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
                    structDataReaders = new Dictionary<string, VectorDataReader>(StringComparer.OrdinalIgnoreCase);

                    for (int index = 0; index < memberCount; index++)
                    {
                        var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index).ToManagedString();
                        var childVector = NativeMethods.DataChunks.DuckDBStructVectorGetChild(vector, index);

                        var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
                        var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

                        using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
                        var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

                        structDataReaders[name] = new VectorDataReader(childVector, childVectorData, childVectorValidity, type);
                    }
                    break;
                }
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

    internal unsafe DateTime GetDateTime(ulong offset)
    {
        if (DuckDBType == DuckDBType.Date)
        {
            return GetDateOnly(offset).ToDateTime();
        }
        var data = (DuckDBTimestampStruct*)dataPointer + offset;
        return data->ToDateTime();
    }

    private unsafe BigInteger GetBigInteger(ulong offset)
    {
        var data = (DuckDBHugeInt*)dataPointer + offset;
        return data->ToBigInteger();
    }

    internal object GetEnum(ulong offset, Type returnType)
    {
        long enumValue = enumType switch
        {
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
            _ => -1
        };

        if (returnType == typeof(string))
        {
            var value = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, enumValue).ToManagedString();
            return value;
        }

        var underlyingType = Nullable.GetUnderlyingType(returnType);
        if (underlyingType != null)
        {
            if (!IsValid(offset))
            {
                return default!;
            }
            returnType = underlyingType;
        }

        var enumItem = Enum.Parse(returnType, enumValue.ToString(CultureInfo.InvariantCulture));
        return enumItem;
    }

    internal object GetStruct(ulong offset, Type returnType)
    {
        if (structDataReaders is null)
        {
            throw new InvalidOperationException("Can't get a struct from a non-struct vector.");
        }

        var result = Activator.CreateInstance(returnType);

        if (result is Dictionary<string, object?> dictionary)
        {
            foreach (var reader in structDataReaders)
            {
                var value = reader.Value.IsValid(offset) ? reader.Value.GetValue(offset) : null;
                dictionary.Add(reader.Key, value);
            }

            return result;
        }

        var properties = returnType.GetProperties();
        foreach (var propertyInfo in properties)
        {
            if (propertyInfo.SetMethod == null)
            {
                continue;
            }

            structDataReaders.TryGetValue(propertyInfo.Name, out var reader);
            var isNotNullable = propertyInfo.PropertyType.IsValueType && Nullable.GetUnderlyingType(propertyInfo.PropertyType) == null;

            if (reader == null)
            {
                if (isNotNullable)
                {
                    throw new NullReferenceException($"Property '{propertyInfo.Name}' not found in struct");
                }

                continue;
            }

            if (reader.IsValid(offset))
            {
                var value = reader.GetValue(offset, propertyInfo.PropertyType);
                propertyInfo.SetValue(result, value);
            }
            else
            {
                if (isNotNullable)
                {
                    throw new NullReferenceException($"Property '{propertyInfo.Name}' is not nullable but struct contains null");
                }
            }
        }

        return result!;
    }

    internal unsafe object GetList(ulong offset, Type returnType)
    {
        if (listDataReader is null)
        {
            throw new InvalidOperationException("Can't get a list from a non-list vector.");
        }

        var listData = (DuckDBListEntry*)dataPointer + offset;

        var listType = returnType.GetGenericArguments()[0];

        var nullableType = Nullable.GetUnderlyingType(listType);
        var allowNulls = !listType.IsValueType || nullableType != null;

        var list = Activator.CreateInstance(returnType) as IList
                      ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        var targetType = nullableType ?? listType;

        for (ulong i = 0; i < listData->Length; i++)
        {
            var childOffset = i + listData->Offset;
            if (listDataReader.IsValid(childOffset))
            {
                var item = listDataReader.GetValue(childOffset, targetType);
                list.Add(item);
            }
            else
            {
                if (allowNulls)
                {
                    list.Add(null);
                }
                else
                {
                    throw new NullReferenceException("The list contains null value");
                }
            }
        }

        return list;
    }

    internal object GetValue(ulong offset, Type? targetType = null)
    {
        return DuckDBType switch
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
            DuckDBType.Date => GetDate(offset, targetType),
            DuckDBType.Time => GetTime(offset, targetType),
            DuckDBType.HugeInt => GetBigInteger(offset),
            DuckDBType.Varchar => GetString(offset),
            DuckDBType.Decimal => GetDecimal(offset),
            DuckDBType.Blob => GetStream(offset),
            DuckDBType.List => GetList(offset, targetType ?? ClrType),
            DuckDBType.Enum => GetEnum(offset, targetType ?? ClrType),
            DuckDBType.Struct => GetStruct(offset, targetType ?? ClrType),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type})")
        };
    }

    private DuckDBTimeOnly GetTimeOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(offset));
    }

    private DuckDBDateOnly GetDateOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(offset));
    }

    private object GetDate(ulong offset, Type? targetType = null)
    {
        var dateOnly = GetDateOnly(offset);
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

    private object GetTime(ulong offset, Type? targetType = null)
    {
        var timeOnly = GetTimeOnly(offset);
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

    public void Dispose()
    {
        listDataReader?.Dispose();
        logicalType?.Dispose();

        if (structDataReaders != null)
        {
            foreach (var item in structDataReaders)
            {
                item.Value.Dispose();
            }
        }
    }
}
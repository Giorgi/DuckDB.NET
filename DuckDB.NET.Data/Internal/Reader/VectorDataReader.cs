using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.Internal.Reader;

internal class VectorDataReader : IDisposable
{
    private const int InlineStringMaxLength = 12;

    private readonly IntPtr vector;
    private readonly unsafe ulong* validityMaskPointer;

    internal Type ClrType { get; set; }
    internal DuckDBType DuckDBType { get; }
    public unsafe void* DataPointer { get; }

    internal unsafe VectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
        this.vector = vector;
        DataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;

        DuckDBType = columnType;

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
            DuckDBType.List => typeof(List<>),
            DuckDBType.Struct => typeof(Dictionary<string, object>),
            var type => throw new ArgumentException($"Unrecognised type {type} ({(int)type})")
        };
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

    protected unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)DataPointer + offset);

    private TResult GetUnmanagedTypeValue<TResult, TQuery>(ulong offset) where TQuery : unmanaged
    {
        var fieldData = GetFieldData<TQuery>(offset);

        return Unsafe.As<TQuery, TResult>(ref fieldData);
    }

    internal virtual decimal GetDecimal(ulong offset)
    {
        throw new InvalidOperationException($"Cannot read Decimal from a non-{nameof(DecimalVectorDataReader)}");
    }

    internal unsafe string GetString(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;
        var length = *(int*)data;

        var pointer = length <= InlineStringMaxLength
            ? data->value.inlined.inlined
            : data->value.pointer.ptr;

        return new string(pointer, 0, length, Encoding.UTF8);
    }

    internal unsafe Stream GetStream(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;
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
        var data = (DuckDBTimestampStruct*)DataPointer + offset;
        return data->ToDateTime();
    }

    protected unsafe BigInteger GetBigInteger(ulong offset)
    {
        var data = (DuckDBHugeInt*)DataPointer + offset;
        return data->ToBigInteger();
    }

    internal virtual object GetEnum(ulong offset, Type returnType)
    {
        throw new InvalidOperationException($"Cannot read Enum from a non-{nameof(EnumVectorDataReader)}");
    }

    internal virtual object GetStruct(ulong offset, Type returnType)
    {
        throw new InvalidOperationException($"Cannot read Struct from a non-{nameof(StructVectorDataReader)}");
    }

    internal virtual object GetList(ulong offset, Type returnType)
    {
        throw new InvalidOperationException($"Cannot read List from a non-{nameof(ListVectorDataReader)}");
    }

    internal T GetValue<T>(ulong offset)
    {
        var targetType = typeof(T);
        var isNullable = default(T) is null && targetType.IsValueType;

        //If nullable we can't use Unsafe.As because we don't have the underlying type as T so use the non-generic GetValue method.
        //Otherwise use the switch below to avoid boxing for numeric types, bool, etc
        if (isNullable)
        {
            return IsValid(offset)
                ? (T)GetValue(offset, targetType)
                : default!; //T is Nullable<> and we are returning null so suppress compiler warning.
        }

        switch (DuckDBType)
        {
            case DuckDBType.Boolean:
                return GetUnmanagedTypeValue<T, bool>(offset);
            case DuckDBType.TinyInt:
                return GetUnmanagedTypeValue<T, sbyte>(offset);
            case DuckDBType.SmallInt:
                return GetUnmanagedTypeValue<T, short>(offset);
            case DuckDBType.Integer:
                return GetUnmanagedTypeValue<T, int>(offset);
            case DuckDBType.BigInt:
                return GetUnmanagedTypeValue<T, long>(offset);
            case DuckDBType.UnsignedTinyInt:
                return GetUnmanagedTypeValue<T, byte>(offset);
            case DuckDBType.UnsignedSmallInt:
                return GetUnmanagedTypeValue<T, ushort>(offset);
            case DuckDBType.UnsignedInteger:
                return GetUnmanagedTypeValue<T, uint>(offset);
            case DuckDBType.UnsignedBigInt:
                return GetUnmanagedTypeValue<T, ulong>(offset);
            case DuckDBType.Float:
                return GetUnmanagedTypeValue<T, float>(offset);
            case DuckDBType.Double:
                return GetUnmanagedTypeValue<T, double>(offset);
            case DuckDBType.Interval:
                return GetUnmanagedTypeValue<T, DuckDBInterval>(offset);
            case DuckDBType.Varchar:
                {
                    var value = GetString(offset);
                    return Unsafe.As<string, T>(ref value);
                }
            case DuckDBType.Decimal:
                {
                    var value = GetDecimal(offset);
                    return Unsafe.As<decimal, T>(ref value);
                }
        }

        return (T)GetValue(offset, targetType);
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

    public virtual void Dispose()
    {
    }
}
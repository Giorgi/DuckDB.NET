using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class VectorDataReaderBase : IDisposable
{
    private readonly unsafe ulong* validityMaskPointer;

    public Type ClrType { get; set; }
    public DuckDBType DuckDBType { get; }
    private protected unsafe void* DataPointer { get; }

    internal unsafe VectorDataReaderBase(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
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

    public unsafe bool IsValid(ulong offset)
    {
        var validityMaskEntryIndex = offset / 64;
        var validityBitIndex = (int)(offset % 64);

        var validityMaskEntryPtr = validityMaskPointer + validityMaskEntryIndex;
        var validityBit = 1ul << validityBitIndex;

        var isValid = (*validityMaskEntryPtr & validityBit) != 0;
        return isValid;
    }

    internal unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)DataPointer + offset);

    internal virtual T GetValue<T>(ulong offset)
    {
        var (isNullable, targetType) = TypeExtensions.IsNullable<T>();

        //If nullable we can't use Unsafe.As because we don't have the underlying type as T so use the non-generic GetValue method.
        //Otherwise use the switch below to avoid boxing for numeric types, bool, etc
        if (isNullable)
        {
            return IsValid(offset)
                ? (T)GetValue(offset, targetType)
                : default!; //T is Nullable<> and we are returning null so suppress compiler warning.
        }

        return GetValueInternal<T>(offset, targetType);
    }

    protected virtual T GetValueInternal<T>(ulong offset, Type targetType)
    {
        return (T)GetValue(offset, targetType);
    }

    internal virtual object GetValue(ulong offset, Type? targetType = null)
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException("Invalid type"),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType})")
        };
    }

    public virtual void Dispose()
    {
    }
}
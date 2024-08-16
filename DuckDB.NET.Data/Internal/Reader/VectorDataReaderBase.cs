using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Reader;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal class VectorDataReaderBase : IDisposable
#if NET8_0_OR_GREATER
#pragma warning disable DuckDBNET001
    , IDuckDBDataReader 
#pragma warning restore DuckDBNET001
#endif
{
    private readonly unsafe ulong* validityMaskPointer;

    private Type? clrType;
    public Type ClrType => clrType ??= GetColumnType();

    private Type? providerSpecificClrType;
    public Type ProviderSpecificClrType => providerSpecificClrType ??= GetColumnProviderSpecificType();


    public string ColumnName { get; }
    public DuckDBType DuckDBType { get; }
    private protected unsafe void* DataPointer { get; }

    internal unsafe VectorDataReaderBase(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName)
    {
        DataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;

        DuckDBType = columnType;
        ColumnName = columnName;
    }

    public unsafe bool IsValid(ulong offset)
    {
        if (validityMaskPointer == default)
        {
            return true;
        }

        var validityMaskEntryIndex = offset / 64;
        var validityBitIndex = (int)(offset % 64);

        var validityMaskEntryPtr = validityMaskPointer + validityMaskEntryIndex;
        var validityBit = 1ul << validityBitIndex;

        var isValid = (*validityMaskEntryPtr & validityBit) != 0;
        return isValid;
    }

    public virtual T GetValue<T>(ulong offset)
    {
        var (isNullableValueType, targetType) = TypeExtensions.IsNullableValueType<T>();

        var isValid = IsValid(offset);

        //If nullable we can't use Unsafe.As because we don't have the underlying type as T so use the non-generic GetValue method.
        if (isNullableValueType)
        {
            return isValid
                ? (T)GetValue(offset, Nullable.GetUnderlyingType(targetType)!)
                : default!; //T is Nullable<> and we are returning null so suppress compiler warning.
        }

        //If we are here, T isn't Nullable<>. It can be either a value type or a class.
        //In both cases if the data is null we should throw.
        if (isValid)
        {
            return GetValidValue<T>(offset, targetType);
        }
        
        throw new InvalidCastException($"Column '{ColumnName}' value is null");
    }

    /// <summary>
    /// Called when the value at specified <param name="offset">offset</param> is valid (isn't null)
    /// </summary>
    /// <typeparam name="T">Type of the return value</typeparam>
    /// <param name="offset">Position to read the data from</param>
    /// <param name="targetType">Type of the return value</param>
    /// <returns>Data at the specified offset</returns>
    protected virtual T GetValidValue<T>(ulong offset, Type targetType)
    {
        return (T)GetValue(offset, targetType);
    }

    public object GetValue(ulong offset)
    {
        return GetValue(offset, ClrType);
    }

    internal virtual object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException($"Invalid type for column {ColumnName}"),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    internal object GetProviderSpecificValue(ulong offset)
    {
        return GetValue(offset, ProviderSpecificClrType);
    }

    protected virtual Type GetColumnType()
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException($"Invalid type for column {ColumnName}"),
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
            DuckDBType.Interval => typeof(TimeSpan),
#if NET6_0_OR_GREATER
            DuckDBType.Date => typeof(DateOnly),
#else
            DuckDBType.Date => typeof(DateTime),
#endif
#if NET6_0_OR_GREATER
            DuckDBType.Time => typeof(TimeOnly),
#else
            DuckDBType.Time => typeof(TimeSpan),
#endif
            DuckDBType.TimeTz => typeof(DateTimeOffset),
            DuckDBType.HugeInt => typeof(BigInteger),
            DuckDBType.UnsignedHugeInt => typeof(BigInteger),
            DuckDBType.Varchar => typeof(string),
            DuckDBType.Decimal => typeof(decimal),
            DuckDBType.TimestampS => typeof(DateTime),
            DuckDBType.TimestampMs => typeof(DateTime),
            DuckDBType.TimestampNs => typeof(DateTime),
            DuckDBType.Blob => typeof(Stream),
            DuckDBType.Enum => typeof(string),
            DuckDBType.Uuid => typeof(Guid),
            DuckDBType.Struct => typeof(Dictionary<string, object>),
            DuckDBType.Bit => typeof(string),
            DuckDBType.TimestampTz => typeof(DateTime),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    protected virtual Type GetColumnProviderSpecificType()
    {
        return DuckDBType switch
        {
            DuckDBType.Invalid => throw new DuckDBException($"Invalid type for column {ColumnName}"),
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
            DuckDBType.Timestamp => typeof(DuckDBTimestamp),
            DuckDBType.Interval => typeof(DuckDBInterval),
            DuckDBType.Date => typeof(DuckDBDateOnly),
            DuckDBType.Time => typeof(DuckDBTimeOnly),
            DuckDBType.TimeTz => typeof(DuckDBTimeTz),
            DuckDBType.HugeInt => typeof(DuckDBHugeInt),
            DuckDBType.UnsignedHugeInt => typeof(DuckDBUHugeInt),
            DuckDBType.Varchar => typeof(string),
            DuckDBType.Decimal => typeof(decimal),
            DuckDBType.TimestampS => typeof(DuckDBTimestamp),
            DuckDBType.TimestampMs => typeof(DuckDBTimestamp),
            DuckDBType.TimestampNs => typeof(DuckDBTimestamp),
            DuckDBType.Blob => typeof(Stream),
            DuckDBType.Enum => typeof(string),
            DuckDBType.Uuid => typeof(Guid),
            DuckDBType.Struct => typeof(Dictionary<string, object>),
            DuckDBType.Bit => typeof(string),
            DuckDBType.TimestampTz => typeof(DuckDBTimestamp),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    protected unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)DataPointer + offset);

    public virtual void Dispose()
    {
    }
}
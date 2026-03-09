using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal class VectorDataReaderBase : IDisposable, IDuckDBDataReader
{
    private unsafe ulong* validityMaskPointer;

    public Type ClrType => field ??= GetColumnType();

    public Type ProviderSpecificClrType => field ??= GetColumnProviderSpecificType();


    public string ColumnName { get; }
    public DuckDBType DuckDBType { get; }
    private protected unsafe void* DataPointer { get; private set; }

    internal unsafe VectorDataReaderBase(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName)
    {
        DataPointer = dataPointer;
        this.validityMaskPointer = validityMaskPointer;

        DuckDBType = columnType;
        ColumnName = columnName;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    public T GetValue<T>(ulong offset) => GetValue<T>(offset, strict: false);

    internal T GetValueStrict<T>(ulong offset) => GetValue<T>(offset, strict: true);

    internal T GetValue<T>(ulong offset, bool strict)
    {
        // When T is Nullable<TUnderlying> (e.g. int?), we can't call GetValidValue<int>() directly
        // because we only have T=int? at compile time. NullableHandler uses a pre-compiled expression
        // tree that calls GetValidValue<int>() and converts to int?, avoiding boxing through the
        // non-generic GetValue(offset, Type) path.
        if (NullableHandler<T>.IsNullableValueType)
        {
            return NullableHandler<T>.Read(this, offset);
        }

        if (IsValid(offset))
        {
            return GetValidValue<T>(offset, typeof(T));
        }

        if (strict || !NullableHandler<T>.IsReferenceType)
        {
            throw new InvalidCastException($"Column '{ColumnName}' value is null");
        }
        return default!;
    }

    /// <summary>
    /// Called when the value at specified <param name="offset">offset</param> is valid (isn't null)
    /// </summary>
    /// <typeparam name="T">Type of the return value</typeparam>
    /// <param name="offset">Position to read the data from</param>
    /// <param name="targetType">Type of the return value</param>
    /// <returns>Data at the specified offset</returns>
    protected virtual T GetValidValue<T>(ulong offset, Type targetType) => (T)GetValue(offset, targetType);

    public object GetValue(ulong offset)
    {
        if (!IsValid(offset)) return null!;
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

    internal object GetProviderSpecificValue(ulong offset) => GetValue(offset, ProviderSpecificClrType);

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
            DuckDBType.Date => typeof(DateOnly),
            DuckDBType.Time => typeof(TimeOnly),
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
            DuckDBType.VarInt => typeof(BigInteger),
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
            DuckDBType.VarInt => typeof(BigInteger),
            _ => throw new ArgumentException($"Unrecognised type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected unsafe T GetFieldData<T>(ulong offset) where T : unmanaged => *((T*)DataPointer + offset);

    /// <summary>
    /// Updates the data and validity pointers for a new chunk without recreating the reader.
    /// Composite readers (Struct, List, Map, Decimal) override this to also reset nested readers.
    /// </summary>
    internal virtual unsafe void Reset(IntPtr vector)
    {
        DataPointer = NativeMethods.Vectors.DuckDBVectorGetData(vector);
        validityMaskPointer = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);
    }

    public virtual void Dispose()
    {
    }

    private static class NullableHandler<T>
    {
        private static Type type;
        private static Type? underlyingType;

        static NullableHandler()
        {
            type = typeof(T);

            var allowsNullValue = type.AllowsNullValue(out IsNullableValueType, out underlyingType);

            Read = IsNullableValueType ? Compile() : null!;
            IsReferenceType = allowsNullValue && !IsNullableValueType;
        }

        public static readonly bool IsNullableValueType;
        public static readonly bool IsReferenceType;
        public static readonly Func<VectorDataReaderBase, ulong, T> Read;

        // For T = int?, builds a delegate equivalent to:
        //   (VectorDataReaderBase reader, ulong offset) =>
        //       reader.IsValid(offset)
        //           ? (int?)reader.GetValidValue<int>(offset, typeof(int))
        //           : default(int?)
        private static Func<VectorDataReaderBase, ulong, T> Compile()
        {
            if (underlyingType is null) return null!;

            var reader = Expression.Parameter(typeof(VectorDataReaderBase));
            var offset = Expression.Parameter(typeof(ulong));

            var isValid = Expression.Call(reader, typeof(VectorDataReaderBase).GetMethod(nameof(IsValid))!, offset);

            var methodInfo = typeof(VectorDataReaderBase).GetMethod(nameof(GetValidValue), BindingFlags.Instance | BindingFlags.NonPublic)!;
            var genericGetValidValue = methodInfo.MakeGenericMethod(underlyingType);

            var getValidValue = Expression.Call(reader, genericGetValidValue, offset, Expression.Constant(underlyingType));

            var body = Expression.Condition(isValid, Expression.Convert(getValidValue, type), Expression.Default(type));

            return Expression.Lambda<Func<VectorDataReaderBase, ulong, T>>(body, reader, offset).Compile();
        }
    }
}
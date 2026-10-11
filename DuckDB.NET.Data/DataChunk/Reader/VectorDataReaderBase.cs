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

    // The .NET type whose memory layout is exactly what the vector holds, or null when every read needs a
    // conversion. A reader sets it once, and a read of that type then skips GetValidValue, which is a generic
    // virtual method and so is looked up at run time on every call.
    private protected Type? StorageType { get; init; }

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

        return ValidityMask.IsValid(validityMaskPointer, offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T GetValue<T>(ulong offset) => GetValue<T>(offset, strict: false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal T GetValueStrict<T>(ulong offset) => GetValue<T>(offset, strict: true);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            return ReadValidValue<T>(offset);
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
    /// <returns>Data at the specified offset</returns>
    protected virtual T GetValidValue<T>(ulong offset) => (T)GetValue(offset, typeof(T));

    // One method for each type that DbDataReader has a getter for. GetValidValue is a generic virtual method, which
    // is looked up at run time on every call. These are plain virtual methods, which are not, and ReadValidValue
    // calls them for their types. A reader overrides the ones it produces, and an override calls GetValidValue of
    // its own class, so a value is converted by the same code whichever way it is reached.
    internal virtual bool ReadBoolean(ulong offset) => GetValidValue<bool>(offset);
    internal virtual byte ReadByte(ulong offset) => GetValidValue<byte>(offset);
    internal virtual short ReadInt16(ulong offset) => GetValidValue<short>(offset);
    internal virtual int ReadInt32(ulong offset) => GetValidValue<int>(offset);
    internal virtual long ReadInt64(ulong offset) => GetValidValue<long>(offset);
    internal virtual float ReadFloat(ulong offset) => GetValidValue<float>(offset);
    internal virtual double ReadDouble(ulong offset) => GetValidValue<double>(offset);
    internal virtual decimal ReadDecimal(ulong offset) => GetValidValue<decimal>(offset);
    internal virtual DateTime ReadDateTime(ulong offset) => GetValidValue<DateTime>(offset);
    internal virtual Guid ReadGuid(ulong offset) => GetValidValue<Guid>(offset);
    internal virtual string ReadString(ulong offset) => GetValidValue<string>(offset);
    internal virtual Stream ReadStream(ulong offset) => GetValidValue<Stream>(offset);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private unsafe T ReadValidValue<T>(ulong offset)
    {
        // typeof(T).IsValueType is a constant for the JIT, so a reference type does not keep this test.
        if (typeof(T).IsValueType && typeof(T) == StorageType)
        {
            return Unsafe.Read<T>((byte*)DataPointer + offset * (ulong)Unsafe.SizeOf<T>());
        }

        // A test of T against a type is a constant for the JIT too, so at most one of these lines is left.
        if (typeof(T) == typeof(bool))     return (T)(object)ReadBoolean(offset);
        if (typeof(T) == typeof(byte))     return (T)(object)ReadByte(offset);
        if (typeof(T) == typeof(short))    return (T)(object)ReadInt16(offset);
        if (typeof(T) == typeof(int))      return (T)(object)ReadInt32(offset);
        if (typeof(T) == typeof(long))     return (T)(object)ReadInt64(offset);
        if (typeof(T) == typeof(float))    return (T)(object)ReadFloat(offset);
        if (typeof(T) == typeof(double))   return (T)(object)ReadDouble(offset);
        if (typeof(T) == typeof(decimal))  return (T)(object)ReadDecimal(offset);
        if (typeof(T) == typeof(DateTime)) return (T)(object)ReadDateTime(offset);
        if (typeof(T) == typeof(Guid))     return (T)(object)ReadGuid(offset);
        if (typeof(T) == typeof(string))   return (T)(object)ReadString(offset);
        if (typeof(T) == typeof(Stream))   return (T)(object)ReadStream(offset);

        return GetValidValue<T>(offset);
    }

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
            DuckDBType.Decimal => typeof(DuckDBDecimal),
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

    // Kept out of the callers: building the message inline puts its buffers in the stack frame of a method
    // that almost never throws, and that frame is cleared on every call.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private protected InvalidCastException CannotCast(Type from, Type to) => CannotCast(from.Name, to);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private protected InvalidCastException CannotCast(string from, Type to) => new($"Cannot cast from {from} to {to.Name} in column {ColumnName}");

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
        //           ? (int?)reader.ReadValidValue<int>(offset)
        //           : default(int?)
        private static Func<VectorDataReaderBase, ulong, T> Compile()
        {
            if (underlyingType is null) return null!;

            var reader = Expression.Parameter(typeof(VectorDataReaderBase));
            var offset = Expression.Parameter(typeof(ulong));

            var isValid = Expression.Call(reader, typeof(VectorDataReaderBase).GetMethod(nameof(IsValid))!, offset);

            var methodInfo = typeof(VectorDataReaderBase).GetMethod(nameof(ReadValidValue), BindingFlags.Instance | BindingFlags.NonPublic)!;
            var genericGetValidValue = methodInfo.MakeGenericMethod(underlyingType);

            var getValidValue = Expression.Call(reader, genericGetValidValue, offset);

            var body = Expression.Condition(isValid, Expression.Convert(getValidValue, type), Expression.Default(type));

            return Expression.Lambda<Func<VectorDataReaderBase, ulong, T>>(body, reader, offset).Compile();
        }
    }
}
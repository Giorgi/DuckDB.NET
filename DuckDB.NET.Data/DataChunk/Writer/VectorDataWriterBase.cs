using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal unsafe class VectorDataWriterBase(IntPtr vector, void* vectorData, DuckDBType columnType) : IDuckDBDataWriter, IDisposable
{
    private ulong* validity;

    internal IntPtr Vector => vector;
    internal DuckDBType ColumnType => columnType;

    public virtual void WriteNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(Vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(Vector);
        }

        ValidityMask.SetInvalid(validity, rowIndex);
    }

    public void WriteValue<T>(T value, ulong rowIndex)
    {
        if (value == null)
        {
            WriteNull(rowIndex);
            return;
        }

        _ = value switch
        {
            bool val => AppendBool(val, rowIndex),

            sbyte val => AppendNumeric(val, rowIndex),
            short val => AppendNumeric(val, rowIndex),
            int val => AppendNumeric(val, rowIndex),
            long val => AppendNumeric(val, rowIndex),

            byte val => AppendNumeric(val, rowIndex),
            ushort val => AppendNumeric(val, rowIndex),
            uint val => AppendNumeric(val, rowIndex),
            ulong val => AppendNumeric(val, rowIndex),

            float val => AppendNumeric(val, rowIndex),
            double val => AppendNumeric(val, rowIndex),

            decimal val => AppendDecimal(val, rowIndex),
            BigInteger val => AppendBigInteger(val, rowIndex),

            Enum val => AppendEnum(val, rowIndex),

            string val => AppendString(val, rowIndex),
            Guid val => AppendGuid(val, rowIndex),
            DateTime val => AppendDateTime(val, rowIndex),
            TimeSpan val => AppendTimeSpan(val, rowIndex),
            DuckDBDateOnly val => AppendDateOnly(val, rowIndex),
            DuckDBTimeOnly val => AppendTimeOnly(val, rowIndex),
            DateOnly val => AppendDateOnly(val, rowIndex),
            TimeOnly val => AppendTimeOnly(val, rowIndex),
            DateTimeOffset val => AppendDateTimeOffset(val, rowIndex),
            ICollection val => AppendCollection(val, rowIndex),
            _ => ThrowException<T>()
        };

        // A value can land where a NULL was written before: a collection that failed part way can have
        // marked some of its items NULL, and retrying it writes the same items again.
        if (validity != default)
        {
            ValidityMask.SetValid(validity, rowIndex);
        }
    }

    internal virtual bool AppendBool(bool value, ulong rowIndex) => ThrowException<bool>();

    internal virtual bool AppendDecimal(decimal value, ulong rowIndex) => ThrowException<decimal>();

    internal virtual bool AppendTimeSpan(TimeSpan value, ulong rowIndex) => ThrowException<TimeSpan>();

    internal virtual bool AppendGuid(Guid value, ulong rowIndex) => ThrowException<Guid>();

    internal virtual bool AppendBlob(byte* value, int length, ulong rowIndex) => ThrowException<byte[]>();

    internal virtual bool AppendString(string value, ulong rowIndex) => ThrowException<string>();

    internal virtual bool AppendDateTime(DateTime value, ulong rowIndex) => ThrowException<DateTime>();

    internal virtual bool AppendDateOnly(DateOnly value, ulong rowIndex) => ThrowException<DateOnly>();

    internal virtual bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => ThrowException<TimeOnly>();

    internal virtual bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => ThrowException<DuckDBDateOnly>();

    internal virtual bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => ThrowException<DuckDBTimeOnly>();

    internal virtual bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex) => ThrowException<DateTimeOffset>();

    // One overload per numeric type instead of a generic virtual method: a generic virtual method is looked up
    // at run time on every call, which costs several times more than storing the value.
    internal virtual bool AppendNumeric(sbyte value, ulong rowIndex) => ThrowException<sbyte>();

    internal virtual bool AppendNumeric(short value, ulong rowIndex) => ThrowException<short>();

    internal virtual bool AppendNumeric(int value, ulong rowIndex) => ThrowException<int>();

    internal virtual bool AppendNumeric(long value, ulong rowIndex) => ThrowException<long>();

    internal virtual bool AppendNumeric(byte value, ulong rowIndex) => ThrowException<byte>();

    internal virtual bool AppendNumeric(ushort value, ulong rowIndex) => ThrowException<ushort>();

    internal virtual bool AppendNumeric(uint value, ulong rowIndex) => ThrowException<uint>();

    internal virtual bool AppendNumeric(ulong value, ulong rowIndex) => ThrowException<ulong>();

    internal virtual bool AppendNumeric(float value, ulong rowIndex) => ThrowException<float>();

    internal virtual bool AppendNumeric(double value, ulong rowIndex) => ThrowException<double>();

    internal virtual bool AppendBigInteger(BigInteger value, ulong rowIndex) => ThrowException<BigInteger>();

    internal virtual bool AppendEnum<TEnum>(TEnum value, ulong rowIndex) where TEnum : Enum => ThrowException<TEnum>();

    internal virtual bool AppendCollection(ICollection value, ulong rowIndex) => ThrowException<ICollection>();

    private bool ThrowException<T>()
    {
        throw new InvalidOperationException($"Cannot write {typeof(T).Name} to {columnType} column");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool AppendValueInternal<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
        return true;
    }

    internal virtual void InitializeWriter()
    {
        // Fetched again rather than cleared: when a list grows, DuckDB keeps the NULLs already marked in
        // its items, and a retried row must still be able to mark them valid. Null when nothing is NULL.
        validity = NativeMethods.Vectors.DuckDBVectorGetValidity(Vector);
        vectorData = NativeMethods.Vectors.DuckDBVectorGetData(Vector);
    }

    public virtual void Dispose()
    {

    }
}

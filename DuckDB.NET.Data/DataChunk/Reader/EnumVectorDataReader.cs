using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class EnumVectorDataReader : VectorDataReaderBase
{
    private readonly DuckDBType enumType;
    private readonly DuckDBLogicalType logicalType;
    private readonly Dictionary<long, string> cachedNames = new(8);

    internal unsafe EnumVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
    }

    internal override string ReadString(ulong offset) => GetValidValue<string>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Enum)
        {
            return base.GetValidValue<T>(offset);
        }

        var index = GetIndex(offset);

        if (typeof(T) == typeof(string))
        {
            return (T)(object)GetName(index);
        }

        if (typeof(T).IsEnum)
        {
            return ToEnum<T>(index);
        }

        if (TypeExtensions.IsIntegralNumericType<T>())
        {
            try
            {
                return NumericConverter.Convert<uint, T>(index);
            }
            catch (OverflowException)
            {
                throw CannotCast(typeof(uint), typeof(T));
            }
        }

        throw CannotCast(typeof(string), typeof(T));
    }

    // The position of the value in the list of names of the enum. DuckDB stores it in 1, 2 or 4 bytes, depending
    // on how many names the enum has.
    private uint GetIndex(ulong offset)
    {
        return enumType switch
        {
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
            _ => throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
        };
    }

    private string GetName(long index)
    {
        if (!cachedNames.TryGetValue(index, out var name))
        {
            cachedNames[index] = name = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, index);
        }

        return name;
    }

    // A C# enum is an integer of 1, 2, 4 or 8 bytes. Unsafe.As reads as many bytes as T has, so it must be given
    // a value of exactly that size: with a smaller one it reads whatever lies next to it in memory.
    private static T ToEnum<T>(uint index)
    {
        switch (Unsafe.SizeOf<T>())
        {
            case 1:
            {
                var value = (byte)index;
                return Unsafe.As<byte, T>(ref value);
            }
            case 2:
            {
                var value = (ushort)index;
                return Unsafe.As<ushort, T>(ref value);
            }
            case 4:
                return Unsafe.As<uint, T>(ref index);
            default:
            {
                var value = (ulong)index;
                return Unsafe.As<ulong, T>(ref value);
            }
        }
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Enum)
        {
            return base.GetValueCore(offset, targetType);
        }

        // The name, which is the .NET type of the column and what GetValue(ordinal) asks for.
        if (targetType == typeof(string))
        {
            return GetName(GetIndex(offset));
        }

        // A C# enum, known here only at run time. Enum.ToObject keeps the bytes that fit the enum, as ToEnum does.
        if (targetType.IsEnum)
        {
            return Enum.ToObject(targetType, GetIndex(offset));
        }

        // The position as an integer type, read with GetValidValue, so a list item, a map key or value and a struct
        // field convert as the column does. The class is sealed, so these calls are bound directly.
        if (targetType == typeof(sbyte))      return GetValidValue<sbyte>(offset);
        if (targetType == typeof(byte))       return GetValidValue<byte>(offset);
        if (targetType == typeof(short))      return GetValidValue<short>(offset);
        if (targetType == typeof(ushort))     return GetValidValue<ushort>(offset);
        if (targetType == typeof(int))        return GetValidValue<int>(offset);
        if (targetType == typeof(uint))       return GetValidValue<uint>(offset);
        if (targetType == typeof(long))       return GetValidValue<long>(offset);
        if (targetType == typeof(ulong))      return GetValidValue<ulong>(offset);
        if (targetType == typeof(BigInteger)) return GetValidValue<BigInteger>(offset);

        // The message the typed path gives for a type it refuses.
        throw CannotCast(typeof(string), targetType);
    }

    public override void Dispose()
    {
        logicalType.Dispose();
        base.Dispose();
    }
}
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// How the keys and values of a Dictionary<TKey, TValue> are read without a box (CreateBuilder and the builders)
// is in MapVectorDataReader.Builders.cs.
internal sealed partial class MapVectorDataReader : VectorDataReaderBase
{
    private readonly VectorDataReaderBase keyReader;
    private readonly VectorDataReaderBase valueReader;

    // What GetValueCore found out about the type it was asked for last. It is worked out once per type it is asked
    // for, as a column is read as the same type row after row.
    private Type? planType;
    private MapPlan plan;

    internal unsafe MapVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, DuckDBLogicalType logicalColumnType, string columnName)
        : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var keyTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeKeyType(logicalColumnType);
        using var valueTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeValueType(logicalColumnType);

        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);

        var keyVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 0);
        var valueVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 1);

        keyReader = VectorDataReaderFactory.CreateReader(keyVector, keyTypeLogical, columnName);
        valueReader = VectorDataReaderFactory.CreateReader(valueVector, valueTypeLogical, columnName);
    }

    protected override Type GetColumnType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ClrType, valueReader.ClrType);
    }

    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ProviderSpecificClrType, valueReader.ProviderSpecificClrType);
    }

    internal override unsafe object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Map)
        {
            return base.GetValueCore(offset, targetType);
        }

        if (targetType != planType)
        {
            plan = CreatePlan(targetType);
            planType = targetType;
        }

        var listData = (DuckDBListEntry*)DataPointer + offset;

        if (plan.Builder != null)
        {
            return plan.Builder.Build(keyReader, valueReader, listData->Offset, listData->Length);
        }

        return GetBoxedDictionary(targetType, listData->Offset, listData->Length);
    }

    // Any type without a builder (see CreateBuilder): created as itself and filled through IDictionary, with every key
    // and value read as an object.
    private IDictionary GetBoxedDictionary(Type targetType, ulong listOffset, ulong length)
    {
        var instance = Activator.CreateInstance(targetType) as IDictionary ?? throw NotADictionaryException();

        for (ulong i = 0; i < length; i++)
        {
            var childOffset = i + listOffset;

            if (!keyReader.IsValid(childOffset))
            {
                ThrowNullKey(ColumnName);
            }

            var key = keyReader.GetValue(childOffset, plan.KeyType);
            var value = valueReader.IsValid(childOffset) ? valueReader.GetValue(childOffset, plan.ValueType) : null;

            if (plan.AllowsNullValues || value != null)
            {
                instance.Add(key, value);
            }
            else
            {
                ThrowNullValue(ColumnName);
            }
        }

        return instance;
    }

    // Returns the exception, for '?? throw', as the list reader's NotAListException does.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private InvalidOperationException NotADictionaryException() => new($"Cannot read Map column {ColumnName} in a non-dictionary type");

    // Used by the IDictionary path and by the builders. A child reader carries the name of the map column.
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNullValue(string columnName) => throw new InvalidCastException($"The Map in column {columnName} contains null value but dictionary does not allow null values");

    // Used by the IDictionary path and by the builders. SQL cannot make a NULL map key, but a map can also arrive
    // from Parquet, Arrow or the C API.
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowNullKey(string columnName) => throw new InvalidCastException($"The Map in column {columnName} contains a null key");

    // Not inlined, so that GetValueCore does not set up a native-call frame on every row (Reader/DESIGN.md).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private MapPlan CreatePlan(Type targetType)
    {
        // The key and value types are those of the Dictionary<TKey, TValue> the target is or derives from, so a class
        // that is not generic itself, or whose type arguments are not the key and value, is read by its base. Another
        // type (a SortedDictionary, a Hashtable) gives its own type arguments, or none.
        var dictionaryType = targetType.FindGenericBase(typeof(Dictionary<,>));
        var arguments = (dictionaryType ?? targetType).GetGenericArguments();

        // What the IDictionary path reads a key and a value as, and whether it adds a NULL value; a builder knows
        // its own types.
        var keyTargetType = keyReader.ClrType;
        var valueTargetType = valueReader.ClrType;
        var allowsNullValues = false;

        if (arguments.Length == 2)
        {
            if (arguments[0] == typeof(object) || arguments[1] == typeof(object))
            {
                throw new InvalidCastException($"Cannot read Map column {ColumnName} as a dictionary with object keys or values. Use a dictionary with concrete key and value types, or GetValue().");
            }

            arguments[0].AllowsNullValue(out _, out var underlyingKeyType);
            keyTargetType = underlyingKeyType ?? arguments[0];

            allowsNullValues = arguments[1].AllowsNullValue(out _, out var underlyingValueType);
            valueTargetType = underlyingValueType ?? arguments[1];
        }

        return new MapPlan(Builder: CreateBuilder(targetType, dictionaryType), KeyType: keyTargetType,
                           ValueType: valueTargetType, AllowsNullValues: allowsNullValues);
    }

    // Builder reads keys and values on the typed path, or is null. KeyType, ValueType and AllowsNullValues are for
    // the IDictionary path: the types a key and a value are read as, and whether a NULL value is added.
    private readonly record struct MapPlan(MapBuilder? Builder, Type KeyType, Type ValueType, bool AllowsNullValues);

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);
        var keyVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 0);
        var valueVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 1);
        keyReader.Reset(keyVector);
        valueReader.Reset(valueVector);
    }

    public override void Dispose()
    {
        keyReader.Dispose();
        valueReader.Dispose();
        base.Dispose();
    }
}
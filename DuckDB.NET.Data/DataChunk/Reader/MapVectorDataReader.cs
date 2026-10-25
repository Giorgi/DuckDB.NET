using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class MapVectorDataReader : VectorDataReaderBase
{
    private readonly VectorDataReaderBase keyReader;
    private readonly VectorDataReaderBase valueReader;

    // What GetValue found out about the type it was asked for last. A column is read as the same type row after row,
    // so it is worked out once. A Dictionary<TKey, TValue>, or a class derived from one, gets a builder that reads
    // keys and values on the typed path; any other type is filled through IDictionary with the plan.
    private Type? planType;
    private MapBuilder? builder;
    private (Type Key, Type Value, bool AllowsNullValues)? plan;

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
            builder = CreateBuilder(targetType);
            plan = null;
            planType = targetType;
        }

        var listData = (DuckDBListEntry*)DataPointer + offset;

        if (builder != null)
        {
            return builder.Build(this, listData->Offset, listData->Length);
        }

        return GetBoxedDictionary(targetType, listData->Offset, listData->Length);
    }

    // Any type that is not a Dictionary<TKey, TValue>: created as itself and filled through IDictionary, with every
    // key and value read as an object.
    private IDictionary GetBoxedDictionary(Type targetType, ulong listOffset, ulong length)
    {
        if (Activator.CreateInstance(targetType) is not IDictionary instance)
        {
            throw new InvalidOperationException($"Cannot read Map column {ColumnName} in a non-dictionary type");
        }

        plan ??= CreatePlan(targetType);

        var (keyTargetType, valueTargetType, allowsNullValues) = plan.Value;

        for (ulong i = 0; i < length; i++)
        {
            var childOffset = i + listOffset;

            var key = keyReader.IsValid(childOffset) ? keyReader.GetValue(childOffset, keyTargetType) : null!;
            var value = valueReader.IsValid(childOffset) ? valueReader.GetValue(childOffset, valueTargetType) : null;

            if (allowsNullValues || value != null)
            {
                instance.Add(key, value);
            }
            else
            {
                ThrowNullValue();
            }
        }

        return instance;
    }

    private (Type Key, Type Value, bool AllowsNullValues) CreatePlan(Type targetType)
    {
        var arguments = targetType.GetGenericArguments();

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

        return (keyTargetType, valueTargetType, allowsNullValues);
    }

    // The builder for a Dictionary<TKey, TValue> or a class derived from one, or null when the type is read through
    // IDictionary. A dictionary with object keys or values, or with a nullable key type, is left to IDictionary, which
    // refuses the first and reads the second as before.
    //
    // A builder is generic over the key and value types, and is found in the same three ways as the nullable reader
    // in VectorDataReaderBase, for applications that are trimmed or compiled ahead of time:
    // - the common pairs are named below, so an application compiled ahead of time has the code for them;
    // - any other pair is made with MakeGenericType where code can be made at run time;
    // - elsewhere the dictionary is filled through IDictionary, as every map was before.
    [MethodImpl(MethodImplOptions.NoInlining)]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(MapBuilder<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableValueMapBuilder<,>))]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The MakeGenericType calls run only where RuntimeFeature.IsDynamicCodeSupported is true, tested in this method; the .NET 8 analyzer does not recognise that guard.")]
    private static MapBuilder? CreateBuilder(Type targetType)
    {
        var dictionaryType = targetType;

        while (dictionaryType != null && !(dictionaryType.IsGenericType && dictionaryType.GetGenericTypeDefinition() == typeof(Dictionary<,>)))
        {
            dictionaryType = dictionaryType.BaseType;
        }

        if (dictionaryType == null)
        {
            return null;
        }

        var arguments = dictionaryType.GetGenericArguments();
        var (keyType, valueType) = (arguments[0], arguments[1]);

        if (keyType == typeof(object) || valueType == typeof(object) || Nullable.GetUnderlyingType(keyType) != null)
        {
            return null;
        }

        var result = KnownBuilder(dictionaryType);

        if (result == null && RuntimeFeature.IsDynamicCodeSupported)
        {
            var underlyingValueType = Nullable.GetUnderlyingType(valueType);

            var builderType = underlyingValueType == null
                ? typeof(MapBuilder<,>).MakeGenericType(keyType, valueType)
                : typeof(NullableValueMapBuilder<,>).MakeGenericType(keyType, underlyingValueType);

            result = (MapBuilder)Activator.CreateInstance(builderType)!;
        }

        if (result != null && targetType != dictionaryType)
        {
            result.DerivedType = targetType;
        }

        return result;
    }

    // Keys of VARCHAR, INTEGER and BIGINT columns with values of the commonest types. Every pair named here is
    // compiled into an application compiled ahead of time, used or not, so the list is kept short.
    private static MapBuilder? KnownBuilder(Type dictionaryType)
    {
        return KnownForKey<string>(dictionaryType) ?? KnownForKey<int>(dictionaryType) ?? KnownForKey<long>(dictionaryType);
    }

    private static MapBuilder? KnownForKey<TKey>(Type dictionaryType) where TKey : notnull
    {
        return Known<TKey, string>(dictionaryType) ?? Known<TKey, int>(dictionaryType) ?? Known<TKey, long>(dictionaryType)
               ?? Known<TKey, double>(dictionaryType) ?? KnownNullable<TKey, int>(dictionaryType)
               ?? KnownNullable<TKey, long>(dictionaryType) ?? KnownNullable<TKey, double>(dictionaryType);
    }

    private static MapBuilder? Known<TKey, TValue>(Type dictionaryType) where TKey : notnull
    {
        return dictionaryType == typeof(Dictionary<TKey, TValue>) ? new MapBuilder<TKey, TValue>() : null;
    }

    private static MapBuilder? KnownNullable<TKey, TValue>(Type dictionaryType) where TKey : notnull where TValue : struct
    {
        return dictionaryType == typeof(Dictionary<TKey, TValue?>) ? new NullableValueMapBuilder<TKey, TValue>() : null;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowNullValue() => throw new InvalidCastException($"The Map in column {ColumnName} contains null value but dictionary does not allow null values");

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowNullKey() => throw new InvalidCastException($"The Map in column {ColumnName} contains a null key");

    private abstract class MapBuilder
    {
        // A class derived from Dictionary<TKey, TValue>, which is created as itself; null for the dictionary type.
        public Type? DerivedType { get; set; }

        public abstract object Build(MapVectorDataReader map, ulong listOffset, ulong length);

        // A Dictionary<TKey, TValue> has room for every entry when it is created; a derived class grows as entries
        // are added.
        [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "DerivedType is the type the caller asked for, which the caller's code refers to, as the IDictionary path creates it with Activator as well.")]
        protected Dictionary<TKey, TValue> Create<TKey, TValue>(ulong length) where TKey : notnull
        {
            if (DerivedType == null)
            {
                return new Dictionary<TKey, TValue>((int)length);
            }

            return (Dictionary<TKey, TValue>)Activator.CreateInstance(DerivedType)!;
        }
    }

    // Keys and values are read with ReadValidValue, the conversion of GetFieldValue<T> on a column, and added
    // without a box. A NULL value is added as null for a reference type and refused for a value type.
    private sealed class MapBuilder<TKey, TValue> : MapBuilder where TKey : notnull
    {
        public override object Build(MapVectorDataReader map, ulong listOffset, ulong length)
        {
            var result = Create<TKey, TValue>(length);
            var keyReader = map.keyReader;
            var valueReader = map.valueReader;

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!keyReader.IsValid(childOffset))
                {
                    map.ThrowNullKey();
                }

                var key = keyReader.ReadValidValue<TKey>(childOffset);

                if (valueReader.IsValid(childOffset))
                {
                    result.Add(key, valueReader.ReadValidValue<TValue>(childOffset));
                }
                else if (!typeof(TValue).IsValueType)
                {
                    result.Add(key, default!);
                }
                else
                {
                    map.ThrowNullValue();
                }
            }

            return result;
        }
    }

    // A dictionary of TValue?: a value is read as TValue, and NULL is added as null.
    private sealed class NullableValueMapBuilder<TKey, TValue> : MapBuilder where TKey : notnull where TValue : struct
    {
        public override object Build(MapVectorDataReader map, ulong listOffset, ulong length)
        {
            var result = Create<TKey, TValue?>(length);
            var keyReader = map.keyReader;
            var valueReader = map.valueReader;

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!keyReader.IsValid(childOffset))
                {
                    map.ThrowNullKey();
                }

                var key = keyReader.ReadValidValue<TKey>(childOffset);

                result.Add(key, valueReader.IsValid(childOffset) ? valueReader.ReadValidValue<TValue>(childOffset) : null);
            }

            return result;
        }
    }

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
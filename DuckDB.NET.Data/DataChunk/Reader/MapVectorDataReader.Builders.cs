using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// How the keys and values of a Dictionary<TKey, TValue> are read without a box. Nothing here depends on the column:
// a builder is given the key and value readers, and the map reader keeps the builder of the last target type in its
// plan.
internal sealed partial class MapVectorDataReader
{
    // The builder for a Dictionary<TKey, TValue> or a class derived from one (dictionaryType, its base), or null when
    // the type is read through IDictionary. CreatePlan has already refused object keys and values; a nullable key type
    // is left to IDictionary, which reads it as the type under it.
    //
    // The key and value types are read from dictionaryType again rather than passed from CreatePlan: passed as a Type
    // parameter, MakeGenericType of NullableValueMapBuilder<,> raises the trim warnings IL2070 and IL2071 (the new()
    // that 'where TValue : struct' implies), which the analyzer does not raise for the result of GetGenericArguments.
    // The list reader takes its element type as a parameter and suppresses IL2071 instead.
    //
    // A builder is generic over the key and value types, and is found in the same three ways as the nullable reader
    // in VectorDataReaderBase, for applications that are trimmed or compiled ahead of time:
    // - the common pairs are named below, so an application compiled ahead of time has the code for them;
    // - any other pair is made with MakeGenericType where code can be made at run time;
    // - elsewhere the dictionary is filled through IDictionary.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(MapBuilder<,>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableValueMapBuilder<,>))]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "The MakeGenericType calls run only where RuntimeFeature.IsDynamicCodeSupported is true, tested in this method; the .NET 8 analyzer does not recognise that guard.")]
    private static MapBuilder? CreateBuilder(Type targetType, Type? dictionaryType)
    {
        if (dictionaryType == null)
        {
            return null;
        }

        var arguments = dictionaryType.GetGenericArguments();
        var (keyType, valueType) = (arguments[0], arguments[1]);

        if (Nullable.GetUnderlyingType(keyType) != null)
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

    // Keys of VARCHAR, INTEGER, BIGINT and UUID columns with values of the commonest types. Every pair named here
    // (each key with each value) is compiled into an application compiled ahead of time, used or not, so the lists
    // are kept short.
    private static MapBuilder? KnownBuilder(Type dictionaryType)
    {
        return KnownForKey<string>(dictionaryType) ?? KnownForKey<int>(dictionaryType) ?? KnownForKey<long>(dictionaryType)
               ?? KnownForKey<Guid>(dictionaryType);
    }

    private static MapBuilder? KnownForKey<TKey>(Type dictionaryType) where TKey : notnull
    {
        return Known<TKey, string>(dictionaryType) ?? Known<TKey, int>(dictionaryType) ?? Known<TKey, long>(dictionaryType)
               ?? Known<TKey, double>(dictionaryType) ?? Known<TKey, bool>(dictionaryType) ?? Known<TKey, DateTime>(dictionaryType)
               ?? Known<TKey, Guid>(dictionaryType) ?? KnownNullable<TKey, int>(dictionaryType)
               ?? KnownNullable<TKey, long>(dictionaryType) ?? KnownNullable<TKey, double>(dictionaryType)
               ?? KnownNullable<TKey, bool>(dictionaryType) ?? KnownNullable<TKey, DateTime>(dictionaryType)
               ?? KnownNullable<TKey, Guid>(dictionaryType);
    }

    private static MapBuilder? Known<TKey, TValue>(Type dictionaryType) where TKey : notnull
    {
        return dictionaryType == typeof(Dictionary<TKey, TValue>) ? new MapBuilder<TKey, TValue>() : null;
    }

    private static MapBuilder? KnownNullable<TKey, TValue>(Type dictionaryType) where TKey : notnull where TValue : struct
    {
        return dictionaryType == typeof(Dictionary<TKey, TValue?>) ? new NullableValueMapBuilder<TKey, TValue>() : null;
    }

    private abstract class MapBuilder
    {
        // A class derived from Dictionary<TKey, TValue>, which is created as itself; null for the dictionary type.
        public Type? DerivedType { get; set; }

        public abstract object Build(VectorDataReaderBase keyReader, VectorDataReaderBase valueReader, ulong listOffset, ulong length);

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
        public override object Build(VectorDataReaderBase keyReader, VectorDataReaderBase valueReader, ulong listOffset, ulong length)
        {
            var result = Create<TKey, TValue>(length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!keyReader.IsValid(childOffset))
                {
                    ThrowNullKey(keyReader.ColumnName);
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
                    ThrowNullValue(valueReader.ColumnName);
                }
            }

            return result;
        }
    }

    // A dictionary of TValue?: a value is read as TValue, and NULL is added as null.
    private sealed class NullableValueMapBuilder<TKey, TValue> : MapBuilder where TKey : notnull where TValue : struct
    {
        public override object Build(VectorDataReaderBase keyReader, VectorDataReaderBase valueReader, ulong listOffset, ulong length)
        {
            var result = Create<TKey, TValue?>(length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!keyReader.IsValid(childOffset))
                {
                    ThrowNullKey(keyReader.ColumnName);
                }

                var key = keyReader.ReadValidValue<TKey>(childOffset);

                result.Add(key, valueReader.IsValid(childOffset) ? valueReader.ReadValidValue<TValue>(childOffset) : null);
            }

            return result;
        }
    }
}
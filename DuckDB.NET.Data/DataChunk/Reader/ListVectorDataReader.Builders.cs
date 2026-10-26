using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// How the items of a List<T> are read without a box. Nothing here depends on the column: a builder is given the
// item reader, and the list reader keeps the builder of the last target type in its plan.
internal sealed partial class ListVectorDataReader
{
    // The builder for a List<T>. As for nullable reads (decision 6 of DESIGN.md), the types an application reads
    // most are named, so that an application compiled ahead of time has the code for them. Any other type, such as
    // an enum of the caller or a nested list, is made through reflection where code can be made at run time, and
    // gets no builder where it cannot.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(ValueListBuilder<>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(NullableListBuilder<>))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(ReferenceListBuilder<>))]
    [UnconditionalSuppressMessage("Trimming", "IL2071", Justification = "The new() constraint that 'where T : struct' implies is met by every value type, and the builders' constructors are kept by the DynamicDependency attributes. The MakeGenericType calls run only where dynamic code is supported.")]
    private static ListBuilder? CreateBuilder(Type elementType)
    {
        var known = Value<bool>(elementType) ?? Value<byte>(elementType) ?? Value<sbyte>(elementType)
                    ?? Value<short>(elementType) ?? Value<ushort>(elementType) ?? Value<int>(elementType) ?? Value<uint>(elementType)
                    ?? Value<long>(elementType) ?? Value<ulong>(elementType) ?? Value<Int128>(elementType) ?? Value<UInt128>(elementType)
                    ?? Value<float>(elementType) ?? Value<double>(elementType) ?? Value<decimal>(elementType) ?? Value<BigInteger>(elementType)
                    ?? Value<DateTime>(elementType) ?? Value<DateTimeOffset>(elementType) ?? Value<DateOnly>(elementType)
                    ?? Value<TimeOnly>(elementType) ?? Value<TimeSpan>(elementType) ?? Value<Guid>(elementType)
                    ?? Value<DuckDBDateOnly>(elementType) ?? Value<DuckDBTimeOnly>(elementType) ?? Value<DuckDBTimeTz>(elementType)
                    ?? Value<DuckDBTimestamp>(elementType) ?? Value<DuckDBInterval>(elementType)
                    ?? Value<DuckDBHugeInt>(elementType) ?? Value<DuckDBUHugeInt>(elementType) ?? Value<DuckDBDecimal>(elementType)
                    ?? Reference<string>(elementType) ?? Reference<Stream>(elementType) ?? Reference<BitArray>(elementType);

        if (known != null)
        {
            return known;
        }

        if (!RuntimeFeature.IsDynamicCodeSupported)
        {
            return null;
        }

        var underlyingType = Nullable.GetUnderlyingType(elementType);

        var builderType = underlyingType != null ? typeof(NullableListBuilder<>).MakeGenericType(underlyingType)
            : elementType.IsValueType ? typeof(ValueListBuilder<>).MakeGenericType(elementType)
            : typeof(ReferenceListBuilder<>).MakeGenericType(elementType);

        return (ListBuilder)Activator.CreateInstance(builderType)!;
    }

    private static ListBuilder? Value<T>(Type elementType) where T : struct
    {
        if (elementType == typeof(T))
        {
            return new ValueListBuilder<T>();
        }

        return elementType == typeof(T?) ? new NullableListBuilder<T>() : null;
    }

    private static ListBuilder? Reference<T>(Type elementType) where T : class
    {
        return elementType == typeof(T) ? new ReferenceListBuilder<T>() : null;
    }

    // Reads the items of a list with ReadValidValue<T> of the item reader, the conversion of GetFieldValue<T> on a
    // column, so an item is not boxed. An item is tested for NULL once, here; ReadValidValue does not test it again.
    private abstract class ListBuilder
    {
        // Fills the list when one is given (a class derived from List<T>), or creates a List<T> with room for every
        // item.
        public abstract IList Build(IList? list, VectorDataReaderBase itemReader, ulong listOffset, ulong length);
    }

    private sealed class ValueListBuilder<T> : ListBuilder where T : struct
    {
        public override IList Build(IList? list, VectorDataReaderBase itemReader, ulong listOffset, ulong length)
        {
            var result = (List<T>?)list ?? new List<T>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!itemReader.IsValid(childOffset))
                {
                    ThrowListContainsNull();
                }

                result.Add(itemReader.ReadValidValue<T>(childOffset));
            }

            return result;
        }
    }

    private sealed class NullableListBuilder<T> : ListBuilder where T : struct
    {
        public override IList Build(IList? list, VectorDataReaderBase itemReader, ulong listOffset, ulong length)
        {
            var result = (List<T?>?)list ?? new List<T?>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                result.Add(itemReader.IsValid(childOffset) ? itemReader.ReadValidValue<T>(childOffset) : null);
            }

            return result;
        }
    }

    private sealed class ReferenceListBuilder<T> : ListBuilder where T : class
    {
        public override IList Build(IList? list, VectorDataReaderBase itemReader, ulong listOffset, ulong length)
        {
            var result = (List<T?>?)list ?? new List<T?>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                result.Add(itemReader.IsValid(childOffset) ? itemReader.ReadValidValue<T>(childOffset) : null);
            }

            return result;
        }
    }
}
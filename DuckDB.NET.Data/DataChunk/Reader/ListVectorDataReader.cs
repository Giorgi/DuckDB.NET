using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class ListVectorDataReader : VectorDataReaderBase
{
    private readonly ulong arraySize;
    private readonly VectorDataReaderBase listDataReader;

    // What GetList found out about the type it was asked for last. A column is read as the same type row after row,
    // so the type is looked at once and not for every row.
    private Type? planType;
    private ListPlan plan;

    public bool IsList => DuckDBType == DuckDBType.List;

    internal unsafe ListVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, DuckDBLogicalType logicalColumnType, string columnName) 
                    : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var childType = IsList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalColumnType) : NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalColumnType);

        var childVector = IsList ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector) : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);

        arraySize = IsList ? 0 : (ulong)NativeMethods.LogicalType.DuckDBArrayVectorGetSize(logicalColumnType);
        listDataReader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
    }

    protected override Type GetColumnType()
    {
        return typeof(List<>).MakeGenericType(listDataReader.ClrType);
    }

    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(List<>).MakeGenericType(listDataReader.ProviderSpecificClrType);
    }

    internal override unsafe object GetValueCore(ulong offset, Type targetType)
    {
        switch (DuckDBType)
        {
            case DuckDBType.List:
                {
                    var listData = (DuckDBListEntry*)DataPointer + offset;

                    return GetList(targetType, listData->Offset, listData->Length);
                }
            case DuckDBType.Array:
                return GetList(targetType, offset * arraySize, arraySize);
            default:
                return base.GetValueCore(offset, targetType);
        }
    }

    private object GetList(Type returnType, ulong listOffset, ulong length)
    {
        if (returnType != planType)
        {
            plan = CreatePlan(returnType);
            planType = returnType;
        }

        // A List<T> itself is created by the builder, with room for every item.
        if (plan.IsExactList)
        {
            return plan.Builder!.Build(null, listDataReader, listOffset, length);
        }

        return BuildOtherList(returnType, listOffset, length);
    }

    // Not inlined, so that GetList does not set up a native-call frame on every row (Reader/DESIGN.md).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ListPlan CreatePlan(Type returnType)
    {
        if (!returnType.IsGenericType)
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {returnType.Name}. Use a generic list with a concrete element type, or GetValue().");
        }

        // The element type is that of the List<T> the target is or derives from, so a class whose type argument is not
        // the element type is read by its base. Another type (a Collection<T>) gives its own type argument.
        var listBase = FindListBase(returnType);
        var listType = (listBase ?? returnType).GetGenericArguments()[0];

        if (listType == typeof(object))
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as a list of object. Use a list with a concrete element type, or GetValue().");
        }

        var allowNulls = listType.AllowsNullValue(out _, out var nullableType);

        // A builder reads the items on the typed path. It serves List<T> and every class derived from it. Without a
        // builder (another collection type, or an element type that needs code the application cannot make), the
        // items are read as objects.
        var builder = listBase != null ? CreateBuilder(listType) : null;

        return new ListPlan(builder, builder != null && listBase == returnType, nullableType ?? listType, allowNulls);
    }

    // The List<T> that the type is or derives from, or null.
    private static Type? FindListBase(Type type)
    {
        for (var current = type; current != null; current = current.BaseType)
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(List<>))
            {
                return current;
            }
        }

        return null;
    }

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
        if (elementType == typeof(T)) return new ValueListBuilder<T>();
        if (elementType == typeof(T?)) return new NullableListBuilder<T>();
        return null;
    }

    private static ListBuilder? Reference<T>(Type elementType) where T : class
    {
        return elementType == typeof(T) ? new ReferenceListBuilder<T>() : null;
    }

    // Any other type: created as itself. A class derived from List<T> is filled by the builder; any other collection
    // is filled through IList, with each item read as an object.
    private IList BuildOtherList(Type returnType, ulong listOffset, ulong length)
    {
        var list = Activator.CreateInstance(returnType) as IList
                   ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        if (plan.Builder != null)
        {
            return plan.Builder.Build(list, listDataReader, listOffset, length);
        }

        return BuildListCommon(list, listOffset, length);
    }

    private IList BuildListCommon(IList result, ulong listOffset, ulong length)
    {
        for (ulong i = 0; i < length; i++)
        {
            var childOffset = listOffset + i;

            if (listDataReader.IsValid(childOffset))
            {
                result.Add(listDataReader.GetValue(childOffset, plan.ItemType));
            }
            else
            {
                if (!plan.AllowNulls)
                {
                    ThrowListContainsNull();
                }

                result.Add(null);
            }
        }

        return result;
    }

    [DoesNotReturn]
    private static void ThrowListContainsNull() => throw new InvalidCastException("The list contains null value");

    // Builder reads the items of a List<T> on the typed path, or is null. IsExactList: the target is List<T> itself,
    // which the builder creates. ItemType is the type an item is read as on the object path: the element type, or
    // the type under a nullable one.
    private readonly record struct ListPlan(ListBuilder? Builder, bool IsExactList, Type ItemType, bool AllowNulls);

    // Reads the items of a list with ReadValidValue<T> of the child reader, the conversion of GetFieldValue<T> on a
    // column, so an item is not boxed. One builder per column; Build is a plain virtual call per row, and the read of
    // an item in its loop is inlined. An item is tested for NULL once, here; ReadValidValue does not test it again.
    private abstract class ListBuilder
    {
        // Fills the list when one is given (a class derived from List<T>), or creates a List<T> with room for every
        // item.
        public abstract IList Build(IList? list, VectorDataReaderBase items, ulong listOffset, ulong length);
    }

    private sealed class ValueListBuilder<T> : ListBuilder where T : struct
    {
        public override IList Build(IList? list, VectorDataReaderBase items, ulong listOffset, ulong length)
        {
            var result = (List<T>?)list ?? new List<T>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                if (!items.IsValid(childOffset))
                {
                    ThrowListContainsNull();
                }

                result.Add(items.ReadValidValue<T>(childOffset));
            }

            return result;
        }
    }

    private sealed class NullableListBuilder<T> : ListBuilder where T : struct
    {
        public override IList Build(IList? list, VectorDataReaderBase items, ulong listOffset, ulong length)
        {
            var result = (List<T?>?)list ?? new List<T?>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                result.Add(items.IsValid(childOffset) ? items.ReadValidValue<T>(childOffset) : null);
            }

            return result;
        }
    }

    private sealed class ReferenceListBuilder<T> : ListBuilder where T : class
    {
        public override IList Build(IList? list, VectorDataReaderBase items, ulong listOffset, ulong length)
        {
            var result = (List<T?>?)list ?? new List<T?>((int)length);

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;

                result.Add(items.IsValid(childOffset) ? items.ReadValidValue<T>(childOffset) : null);
            }

            return result;
        }
    }

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        var childVector = IsList
            ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector)
            : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);
        listDataReader.Reset(childVector);
    }

    public override void Dispose()
    {
        listDataReader.Dispose();
        base.Dispose();
    }
}
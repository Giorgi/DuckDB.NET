using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// How the items of a List<T> are read without a box (CreateBuilder and the builders) is in
// ListVectorDataReader.Builders.cs.
internal sealed partial class ListVectorDataReader : VectorDataReaderBase
{
    private readonly ulong arraySize;
    private readonly VectorDataReaderBase itemReader;

    // What GetList found out about the type it was asked for last. It is worked out once per type it is asked for,
    // as a column is read as the same type row after row.
    private Type? planType;
    private ListPlan plan;

    private bool IsList => DuckDBType == DuckDBType.List;

    internal unsafe ListVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, DuckDBLogicalType logicalColumnType, string columnName)
        : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var childType = IsList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalColumnType) : NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalColumnType);

        var childVector = IsList ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector) : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);

        arraySize = IsList ? 0 : (ulong)NativeMethods.LogicalType.DuckDBArrayVectorGetSize(logicalColumnType);
        itemReader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
    }

    protected override Type GetColumnType()
    {
        return typeof(List<>).MakeGenericType(itemReader.ClrType);
    }

    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(List<>).MakeGenericType(itemReader.ProviderSpecificClrType);
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

    private object GetList(Type targetType, ulong listOffset, ulong length)
    {
        if (targetType != planType)
        {
            plan = CreatePlan(targetType);
            planType = targetType;
        }

        // Three paths:
        // - a List<T> with a builder: created by the builder, with room for every item;
        // - a class derived from List<T> with a builder: created as itself and filled by the builder (GetOtherList);
        // - any other collection, or a List<T> or derived class without a builder: filled through IList (GetOtherList,
        //   FillBoxedList).
        if (plan.IsExactList)
        {
            return plan.Builder!.Build(null, itemReader, listOffset, length);
        }

        return GetOtherList(targetType, listOffset, length);
    }

    // Any type but a List<T> with a builder: created as itself, and filled by the builder if it has one.
    private IList GetOtherList(Type targetType, ulong listOffset, ulong length)
    {
        var list = Activator.CreateInstance(targetType) as IList ?? throw NotAListException(targetType);

        if (plan.Builder != null)
        {
            return plan.Builder.Build(list, itemReader, listOffset, length);
        }

        return FillBoxedList(list, listOffset, length);
    }

    // Without a builder: each item is read as an object of the plan's ItemType and added through IList.
    private IList FillBoxedList(IList result, ulong listOffset, ulong length)
    {
        for (ulong i = 0; i < length; i++)
        {
            var childOffset = listOffset + i;

            if (itemReader.IsValid(childOffset))
            {
                result.Add(itemReader.GetValue(childOffset, plan.ItemType));
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

    // Returns the exception, for '?? throw': a [DoesNotReturn] helper called in an 'is not IList list' test would leave
    // list unassigned for the compiler.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ArgumentException NotAListException(Type targetType) => new($"Cannot read column {ColumnName} as {targetType.Name}: it is not an IList.");

    // Used by FillBoxedList and by ValueListBuilder.
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowListContainsNull() => throw new InvalidCastException("The list contains null value");

    // Not inlined, so that GetList does not set up a native-call frame on every row (Reader/DESIGN.md).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ListPlan CreatePlan(Type targetType)
    {
        // The element type is that of the List<T> the target is or derives from, so a class that is not generic itself,
        // or whose type argument is not the element type, is read by its base. Another type (a Collection<T>) gives its
        // own type argument, and one with none is refused.
        var listBase = FindListBase(targetType);

        if (listBase == null && !targetType.IsGenericType)
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {targetType.Name}. Use a List<T>, a class derived from one, or another generic collection with a concrete element type; or use GetValue().");
        }

        var elementType = (listBase ?? targetType).GetGenericArguments()[0];

        if (elementType == typeof(object))
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as a list of object. Use a list with a concrete element type, or GetValue().");
        }

        var allowNulls = elementType.AllowsNullValue(out _, out var underlyingType);

        // Only a List<T> or a class derived from one has a builder, and none where no code can be made at run time for
        // an element type that CreateBuilder does not name.
        var builder = listBase != null ? CreateBuilder(elementType) : null;

        return new ListPlan(builder, builder != null && listBase == targetType, underlyingType ?? elementType, allowNulls);
    }

    // Builder reads the items of a List<T> on the typed path, or is null. IsExactList: the target is List<T> itself,
    // which the builder creates. ItemType is the type an item is read as on the object path: the element type, or
    // the type under a nullable one.
    private readonly record struct ListPlan(ListBuilder? Builder, bool IsExactList, Type ItemType, bool AllowNulls);

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

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        var childVector = IsList
            ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector)
            : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);
        itemReader.Reset(childVector);
    }

    public override void Dispose()
    {
        itemReader.Dispose();
        base.Dispose();
    }
}
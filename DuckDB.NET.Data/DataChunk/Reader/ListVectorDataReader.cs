using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

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

    /// <summary>
    /// A reader for a LIST or ARRAY column of a V2 result: the element type is the type's first parameter, and an
    /// ARRAY's size its second.
    /// </summary>
    internal unsafe ListVectorDataReader(_duckdb_v2_logical_type* type, DuckDBType columnType, string columnName)
        : base(null, null, columnType, columnName)
    {
        using var childType = DuckDBV2LogicalTypes.GetTypeParam(type, 0, out _);
        using var lease = childType.Acquire();

        arraySize = IsList ? 0 : (ulong)DuckDBV2LogicalTypes.GetBigIntParam(type, 1);
        itemReader = VectorDataReaderFactory.CreateReader(lease.Pointer, columnName);
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
        // CreatePlan refused a type that cannot be created or is not an IList.
        var list = (IList)Activator.CreateInstance(targetType)!;

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
        var listBase = targetType.FindGenericBase(typeof(List<>));

        if (listBase == null && !targetType.IsGenericType)
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {targetType.Name}. Use a List<T>, a class derived from one, or another generic collection with a concrete element type; or use GetValue().");
        }

        var elementType = (listBase ?? targetType).GetGenericArguments()[0];

        if (elementType == typeof(object))
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as a list of object. Use a list with a concrete element type, or GetValue().");
        }

        // Any target but a List<T> is created as itself, so it has to be a class that can be created and filled.
        if (targetType.IsInterface || targetType.IsAbstract)
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {targetType.Name}: it is an interface or an abstract class.");
        }

        if (!typeof(IList).IsAssignableFrom(targetType))
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {targetType.Name}: it is not an IList.");
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

    internal override unsafe void Reset(_duckdb_v2_vector* vector)
    {
        // The list or array is flattened here first, so its entries index its elements as V1's did.
        base.Reset(vector);
        itemReader.Reset(DuckDBV2Vectors.GetChild(vector, 0));
    }

    public override void Dispose()
    {
        itemReader.Dispose();
        base.Dispose();
    }
}
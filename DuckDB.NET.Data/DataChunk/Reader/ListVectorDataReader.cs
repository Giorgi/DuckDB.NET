using System.Diagnostics.CodeAnalysis;
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

    internal override unsafe object GetValue(ulong offset, Type targetType)
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
                return base.GetValue(offset, targetType);
        }
    }

    private object GetList(Type returnType, ulong listOffset, ulong length)
    {
        if (returnType != planType)
        {
            plan = CreatePlan(returnType);
            planType = returnType;
        }

        // The eight types read without boxing an item. The list is created with room for every item.
        var capacity = (int)length;

        return plan.Kind switch
        {
            ListKind.Int32 => BuildList(new List<int>(capacity), listOffset, length),
            ListKind.NullableInt32 => BuildNullableList(new List<int?>(capacity), listOffset, length),
            ListKind.Float => BuildList(new List<float>(capacity), listOffset, length),
            ListKind.NullableFloat => BuildNullableList(new List<float?>(capacity), listOffset, length),
            ListKind.Double => BuildList(new List<double>(capacity), listOffset, length),
            ListKind.NullableDouble => BuildNullableList(new List<double?>(capacity), listOffset, length),
            ListKind.Decimal => BuildList(new List<decimal>(capacity), listOffset, length),
            ListKind.NullableDecimal => BuildNullableList(new List<decimal?>(capacity), listOffset, length),
            _ => BuildOtherList(returnType, listOffset, length)
        };
    }

    // Not inlined: the reflection in it makes GetList set up a frame for a native call, on every row, when it is.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ListPlan CreatePlan(Type returnType)
    {
        if (!returnType.IsGenericType)
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as {returnType.Name}. Use a generic list with a concrete element type, or GetValue().");
        }

        var listType = returnType.GetGenericArguments()[0];

        if (listType == typeof(object))
        {
            throw new InvalidCastException($"Cannot read column {ColumnName} as a list of object. Use a list with a concrete element type, or GetValue().");
        }

        var allowNulls = listType.AllowsNullValue(out _, out var nullableType);

        // Only the list types themselves. A class derived from one of them is created as itself, below.
        var kind = returnType == typeof(List<int>) ? ListKind.Int32
            : returnType == typeof(List<int?>) ? ListKind.NullableInt32
            : returnType == typeof(List<float>) ? ListKind.Float
            : returnType == typeof(List<float?>) ? ListKind.NullableFloat
            : returnType == typeof(List<double>) ? ListKind.Double
            : returnType == typeof(List<double?>) ? ListKind.NullableDouble
            : returnType == typeof(List<decimal>) ? ListKind.Decimal
            : returnType == typeof(List<decimal?>) ? ListKind.NullableDecimal
            : ListKind.Other;

        return new ListPlan(kind, nullableType ?? listType, allowNulls);
    }

    // Any other type: created as itself and filled through IList, with each item read as an object. A class derived
    // from one of the eight list types still gets the typed loop.
    private IList BuildOtherList(Type returnType, ulong listOffset, ulong length)
    {
        var list = Activator.CreateInstance(returnType) as IList
                   ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        return list switch
        {
            List<int> theList => BuildList(theList, listOffset, length),
            List<int?> theList => BuildNullableList(theList, listOffset, length),
            List<float> theList => BuildList(theList, listOffset, length),
            List<float?> theList => BuildNullableList(theList, listOffset, length),
            List<double> theList => BuildList(theList, listOffset, length),
            List<double?> theList => BuildNullableList(theList, listOffset, length),
            List<decimal> theList => BuildList(theList, listOffset, length),
            List<decimal?> theList => BuildNullableList(theList, listOffset, length),
            _ => BuildListCommon(list, listOffset, length)
        };
    }

    // An item is tested for NULL once, here; ReadValidValue does not test it again.
    private List<T> BuildList<T>(List<T> result, ulong listOffset, ulong length)
    {
        for (ulong i = 0; i < length; i++)
        {
            var childOffset = listOffset + i;

            if (!listDataReader.IsValid(childOffset))
            {
                ThrowListContainsNull();
            }

            result.Add(listDataReader.ReadValidValue<T>(childOffset));
        }

        return result;
    }

    private List<T?> BuildNullableList<T>(List<T?> result, ulong listOffset, ulong length) where T : struct
    {
        for (ulong i = 0; i < length; i++)
        {
            var childOffset = listOffset + i;

            result.Add(listDataReader.IsValid(childOffset) ? listDataReader.ReadValidValue<T>(childOffset) : null);
        }

        return result;
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

    private enum ListKind { Other, Int32, NullableInt32, Float, NullableFloat, Double, NullableDouble, Decimal, NullableDecimal }

    // ItemType is the type an item is read as on the object path: the element type, or the type under a nullable one.
    private readonly record struct ListPlan(ListKind Kind, Type ItemType, bool AllowNulls);

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
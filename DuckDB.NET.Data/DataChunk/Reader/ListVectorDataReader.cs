namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class ListVectorDataReader : VectorDataReaderBase
{
    private readonly ulong arraySize;
    private readonly VectorDataReaderBase listDataReader;
    private Type? cachedListType;
    private IListFactory? cachedListFactory;

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

        var list = CreateList(returnType, length);

        //Special case for specific types to avoid boxing
        return list switch
        {
            List<int> theList => BuildList(theList),
            List<int?> theList => BuildList(theList),
            List<float> theList => BuildList(theList),
            List<float?> theList => BuildList(theList),
            List<double> theList => BuildList(theList),
            List<double?> theList => BuildList(theList),
            List<decimal> theList => BuildList(theList),
            List<decimal?> theList => BuildList(theList),
            _ => BuildListCommon(list, nullableType ?? listType)
        };

        List<T> BuildList<T>(List<T> result)
        {
            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;
                if (listDataReader.IsValid(childOffset))
                {
                    var item = listDataReader.GetValueStrict<T>(childOffset);
                    result.Add(item);
                }
                else
                {
                    result.Add(allowNulls ? default! : throw new InvalidCastException("The list contains null value"));
                }
            }
            return result;
        }

        IList BuildListCommon(IList result, Type targetType)
        {
            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;
                if (listDataReader.IsValid(childOffset))
                {
                    var item = listDataReader.GetValue(childOffset, targetType);
                    result.Add(item);
                }
                else
                {
                    result.Add(allowNulls ? null : throw new InvalidCastException("The list contains null value"));
                }
            }
            return result;
        }
    }

    private IList CreateList(Type returnType, ulong length)
    {
        if (returnType != cachedListType)
        {
            cachedListType = returnType;
            cachedListFactory = returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(List<>)
                ? CreateListFactory(returnType)
                : null;
        }

        if (cachedListFactory != null)
        {
            return cachedListFactory.Create(checked((int)length));
        }

        return Activator.CreateInstance(returnType) as IList
               ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");
    }

    private static IListFactory CreateListFactory(Type returnType)
    {
        var factoryType = typeof(ListFactory<>).MakeGenericType(returnType.GetGenericArguments()[0]);
        return (IListFactory)Activator.CreateInstance(factoryType)!;
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

    private interface IListFactory
    {
        IList Create(int capacity);
    }

    private sealed class ListFactory<T> : IListFactory
    {
        public IList Create(int capacity) => new List<T>(capacity);
    }
}

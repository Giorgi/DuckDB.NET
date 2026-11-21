using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;

namespace DuckDB.NET.Data;

public record ColumnInfo(string Name, Type Type);

/// <summary>
/// The number of rows a table function returns, for the optimizer of DuckDB.
/// </summary>
/// <param name="Value">The number of rows.</param>
/// <param name="IsExact">
/// <c>false</c>, the default: <paramref name="Value"/> is an estimate, which the optimizer uses for planning only.
/// <c>true</c>: the function returns exactly <paramref name="Value"/> rows, and DuckDB also takes it as the most
/// rows the function can return. DuckDB relies on that maximum: a function that returns more rows than it said can
/// get a wrong <c>sum</c>, or a <c>LIMIT</c> that is not applied. So pass <c>true</c> only for a count that is
/// certain.
/// </param>
public record CardinalityHint(ulong Value, bool IsExact = false);

public record ProjectedColumn(int Index, string Name, Type Type);

public record TableFunction
{
    public IReadOnlyList<ColumnInfo> Columns { get; }
    public CardinalityHint? Cardinality { get; }
    internal IEnumerable? Data { get; }
    internal Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; }

    public TableFunction(IReadOnlyList<ColumnInfo> columns, IEnumerable data, CardinalityHint? cardinality = null)
    {
        Columns = columns;
        Data = data;
        Cardinality = cardinality;
    }

    internal TableFunction(IReadOnlyList<ColumnInfo> columns, Func<IReadOnlyList<ProjectedColumn>, IEnumerable> dataFactory, CardinalityHint? cardinality = null)
    {
        Columns = columns;
        DataFactory = dataFactory;
        Cardinality = cardinality;
    }
}

// The registration itself is in Functions/TableFunctionRegistration.cs, which DuckDB.ExtensionKit shares.
partial class DuckDBConnection
{
    public void RegisterTableFunction(string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, (_) => resultCallback(), mapperCallback, Array.Empty<Type>());
    }

    public void RegisterTableFunction<T>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T));
    }

    public void RegisterTableFunction<T1, T2>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2));
    }

    public void RegisterTableFunction<T1, T2, T3>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));
    }

    public void RegisterTableFunction<T1, T2, T3, T4>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));
    }

    public void RegisterTableFunction<T1, T2, T3, T4, T5>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));
    }

    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));
    }

    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));
    }

    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));
    }

    public void RegisterTableFunction(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params DuckDBType[] parameterTypes)
    {
        var logicalTypes = Array.ConvertAll(parameterTypes, NativeMethods.LogicalType.DuckDBCreateLogicalType);
        TableFunctionRegistration.Register(NativeConnection, name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }

    private void RegisterTableFunctionInternal(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var logicalTypes = Array.ConvertAll(parameterTypes, TypeExtensions.GetLogicalType);
        TableFunctionRegistration.Register(NativeConnection, name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }
}

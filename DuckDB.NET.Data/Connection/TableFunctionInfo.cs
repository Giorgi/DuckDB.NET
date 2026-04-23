using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, Action<object?, IDuckDBDataWriter[], ulong> mapper, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; } = bind;
    public Action<object?, IDuckDBDataWriter[], ulong> Mapper { get; } = mapper;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerator? dataEnumerator, Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? dataFactory, ulong connectionId) : IDisposable
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerator? DataEnumerator { get; } = dataEnumerator;
    public Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; } = dataFactory;
    public ulong ConnectionId { get; } = connectionId;

    public void Dispose()
    {
        (DataEnumerator as IDisposable)?.Dispose();
    }
}

class TableFunctionInitData(int[] projected, IEnumerator? factoryEnumerator) : IDisposable
{
    public int[] Projected { get; } = projected;
    public IEnumerator? FactoryEnumerator { get; } = factoryEnumerator;

    public void Dispose()
    {
        (FactoryEnumerator as IDisposable)?.Dispose();
    }
}

using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, Action<object?, IDuckDBDataWriter[], ulong> mapper, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; } = bind;
    public Action<object?, IDuckDBDataWriter[], ulong> Mapper { get; } = mapper;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerable? data, Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? dataFactory, ulong connectionId)
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerable? Data { get; } = data;
    public Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; } = dataFactory;
    public ulong ConnectionId { get; } = connectionId;
}

class TableFunctionInitData(int[] projected, IEnumerator? enumerator) : IDisposable
{
    public int[] Projected { get; } = projected;
    public IEnumerator? Enumerator { get; } = enumerator;

    public void Dispose()
    {
        (Enumerator as IDisposable)?.Dispose();
    }
}

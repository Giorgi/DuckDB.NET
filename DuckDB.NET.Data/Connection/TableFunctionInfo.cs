using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, Action<object?, VectorDataWriterBase[], ulong> mapper, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; private set; } = bind;
    public Action<object?, VectorDataWriterBase[], ulong> Mapper { get; private set; } = mapper;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerator dataEnumerator) : IDisposable
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerator DataEnumerator { get; } = dataEnumerator;

    public void Dispose()
    {
        (DataEnumerator as IDisposable)?.Dispose();
    }
}
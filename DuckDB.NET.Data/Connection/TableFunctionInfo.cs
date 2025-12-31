using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> bind, Action<object?, VectorDataWriterBase[], ulong> mapper)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> Bind { get; private set; } = bind;
    public Action<object?, VectorDataWriterBase[], ulong> Mapper { get; private set; } = mapper;
}

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerator dataEnumerator) : IDisposable
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerator DataEnumerator { get; private set; } = dataEnumerator;

    public void Dispose()
    {
        (DataEnumerator as IDisposable)?.Dispose();
    }
}
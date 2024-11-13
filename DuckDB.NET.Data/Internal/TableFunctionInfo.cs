using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DuckDB.NET.Data.Internal;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, Task<TableFunction>> bind, Action<object?, VectorDataWriterBase[], ulong> mapper)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, Task<TableFunction>> Bind { get; private set; } = bind;
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
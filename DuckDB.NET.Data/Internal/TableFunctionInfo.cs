using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;

namespace DuckDB.NET.Data.Internal;

class TableFunctionInfo(Func<IEnumerable<IDuckDBValueReader>, TableFunction> bind, Action<object?, VectorDataWriterBase[]> mapper)
{
    public Func<IEnumerable<IDuckDBValueReader>, TableFunction> Bind { get; private set; } = bind;
    public Action<object?, VectorDataWriterBase[]> Mapper { get; private set; } = mapper;
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
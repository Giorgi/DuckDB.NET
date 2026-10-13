using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

public record ColumnInfo(string Name, Type Type);

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
        RegisterTableFunctionInternal(name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }

    private void RegisterTableFunctionInternal(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var logicalTypes = Array.ConvertAll(parameterTypes, TypeExtensions.GetLogicalType);
        RegisterTableFunctionInternal(name, (positional, _) => resultCallback(positional), mapperCallback, logicalTypes, []);
    }

    internal void RegisterTableFunctionInternal(string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, DuckDBLogicalType[] positionalLogicalTypes, NamedParameterDefinition[] namedParameters)
    {
        RegisterTableFunctionInternal(name, resultCallback, data => new ObjectTableFunctionScan(data.GetEnumerator(), mapperCallback), positionalLogicalTypes, namedParameters);
    }

    internal unsafe void RegisterTableFunctionInternal(string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, Func<IEnumerable, TableFunctionScan> createScan, DuckDBLogicalType[] positionalLogicalTypes, NamedParameterDefinition[] namedParameters)
    {
        var function = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, name);

        foreach (var logicalType in positionalLogicalTypes)
        {
            NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
            logicalType.Dispose();
        }

        foreach (var param in namedParameters)
        {
            using var logicalType = param.Type.GetLogicalType();
            NativeMethods.TableFunction.DuckDBTableFunctionAddNamedParameter(function, param.Name, logicalType);
        }

        var tableFunctionInfo = new TableFunctionInfo(resultCallback, createScan, Array.ConvertAll(namedParameters, p => p.Name));

        NativeMethods.TableFunction.DuckDBTableFunctionSetBind(function, &Bind);
        NativeMethods.TableFunction.DuckDBTableFunctionSetInit(function, &Init);
        NativeMethods.TableFunction.DuckDBTableFunctionSetFunction(function, &TableFunction);
        NativeMethods.TableFunction.DuckDBTableFunctionSetExtraInfo(function, tableFunctionInfo.ToHandle(), &DestroyExtraInfo);
        NativeMethods.TableFunction.DuckDBTableFunctionSupportsProjectionPushdown(function, true);

        var state = NativeMethods.TableFunction.DuckDBRegisterTableFunction(NativeConnection, function);

        NativeMethods.TableFunction.DuckDBDestroyTableFunction(ref function);

        if (!state.IsSuccess())
        {
            throw new InvalidOperationException($"Error registering user defined table function: {name}");
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void Bind(IntPtr info)
    {
        IDuckDBValueReader[] parameters = [];
        Dictionary<string, IDuckDBValueReader> named = [];
        try
        {
            var handle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info));

            if (handle.Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
            }

            parameters = new IDuckDBValueReader[NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

            for (var i = 0; i < parameters.Length; i++)
            {
                var value = NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
                parameters[i] = value;
            }

            // When a named parameter is omitted in SQL, duckdb_bind_get_named_parameter returns a null pointer.
            // We substitute it with NullValueReader so CompileValueReader's IsNull() check
            // correctly handles it (returns default for nullable, throws for non-nullable).
            foreach (var paramName in functionInfo.NamedParameterNames)
            {
                var value = NativeMethods.TableFunction.DuckDBBindGetNamedParameter(info, paramName);
                named[paramName] = value.IsInvalid ? NullValueReader.Instance : value;
            }

            var tableFunctionData = functionInfo.Bind(parameters, named);

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var logicalType = columnInfo.Type.GetLogicalType();
                NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnInfo.Name, logicalType);
            }

            if (tableFunctionData.Cardinality is { } cardinality)
            {
                NativeMethods.TableFunction.DuckDBBindSetCardinality(info, cardinality.Value, isExact: cardinality.IsExact);
            }

            var connectionId = UdfExceptionStore.GetTableFunctionBindConnectionId(info);
            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data, tableFunctionData.DataFactory, connectionId);

            NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            try
            {
                var connectionId = UdfExceptionStore.GetTableFunctionBindConnectionId(info);
                UdfExceptionStore.Store(connectionId, ex);
            }
            catch
            {
                // If we can't get the connection ID, we still report the error message
            }

            NativeMethods.TableFunction.DuckDBBindSetError(info, ex.Message);
        }
        finally
        {
            foreach (var parameter in parameters)
            {
                (parameter as IDisposable)?.Dispose();
            }

            foreach (var namedParam in named.Values)
            {
                (namedParam as IDisposable)?.Dispose();
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void Init(IntPtr info)
    {
        TableFunctionBindData? bindData = null;
        try
        {
            var bindHandle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBInitGetBindData(info));
            if (bindHandle.Target is not TableFunctionBindData data)
            {
                throw new InvalidOperationException("User defined table function init failed. Bind data is null");
            }
            bindData = data;

            var count = (int)NativeMethods.TableFunction.DuckDBInitGetColumnCount(info);
            var projected = new int[count];
            for (var i = 0; i < count; i++)
            {
                projected[i] = (int)NativeMethods.TableFunction.DuckDBInitGetColumnIndex(info, (ulong)i);
            }

            if (GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBInitGetExtraInfo(info)).Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function init failed. Function extra info is null");
            }

            // The scan must be created per init, not per bind: DuckDB re-initializes a scan
            // without re-binding when it re-executes a pipeline (e.g. on every iteration of a
            // recursive CTE), and each scan must read the data from the beginning.
            IEnumerable? rows;
            if (bindData.DataFactory is { } factory)
            {
                var projectedColumns = new ProjectedColumn[count];
                for (var i = 0; i < count; i++)
                {
                    var column = bindData.Columns[projected[i]];
                    projectedColumns[i] = new ProjectedColumn(projected[i], column.Name, column.Type);
                }

                rows = factory(projectedColumns);
            }
            else
            {
                rows = bindData.Data;
            }

            // The scan is disposed in TableFunctionInitData.Dispose.
            var scan = rows is null ? null : functionInfo.CreateScan(rows);

            var logicalTypes = new DuckDBLogicalType[count];
            for (var i = 0; i < count; i++)
            {
                logicalTypes[i] = bindData.Columns[projected[i]].Type.GetLogicalType();
            }

            var initData = new TableFunctionInitData(projected, scan, logicalTypes, bindData.Columns.Count);
            NativeMethods.TableFunction.DuckDBInitSetInitData(info, initData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            if (bindData is not null)
            {
                UdfExceptionStore.Store(bindData.ConnectionId, ex);
            }
            NativeMethods.TableFunction.DuckDBInitSetError(info, ex.Message);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void TableFunction(IntPtr info, IntPtr chunk)
    {
        IDuckDBDataWriter[] writers = [];
        try
        {
            var initHandle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetInitData(info));

            if (initHandle.Target is not TableFunctionInitData tableFunctionInitData)
            {
                throw new InvalidOperationException("User defined table function failed. Function init data is null");
            }

            var dataChunk = new DuckDBDataChunk(chunk);
            var projected = tableFunctionInitData.Projected;

            writers = tableFunctionInitData.Writers;
            Array.Fill(writers, NullDataWriter.Instance);

            for (var columnIndex = 0; columnIndex < projected.Length; columnIndex++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, columnIndex);

                writers[projected[columnIndex]] = VectorDataWriterFactory.CreateWriter(vector, tableFunctionInitData.LogicalTypes[columnIndex]);
            }

            if (tableFunctionInitData.Scan is not { } scan)
            {
                throw new InvalidOperationException("User defined table function failed. No data source available");
            }

            var size = scan.Fill(writers, DuckDBGlobalData.VectorSize);

            NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
        }
        catch (Exception ex)
        {
            try
            {
                var bindData = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetBindData(info));
                if (bindData.Target is TableFunctionBindData tableFunctionBindData)
                {
                    UdfExceptionStore.Store(tableFunctionBindData.ConnectionId, ex);
                }
            }
            catch
            {
                // If we can't get the connection ID, we still report the error message
            }

            NativeMethods.TableFunction.DuckDBFunctionSetError(info, ex.Message);
        }
        finally
        {
            foreach (var writer in writers)
            {
                (writer as IDisposable)?.Dispose();
            }
        }
    }

    private class NullValueReader : IDuckDBValueReader
    {
        public static readonly NullValueReader Instance = new();
        public bool IsNull() => true;
        public T GetValue<T>() => throw new InvalidOperationException("Cannot read value from a null parameter.");
    }
}
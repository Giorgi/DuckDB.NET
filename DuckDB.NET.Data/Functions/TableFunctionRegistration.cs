using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data.Functions;

// Every table function is registered here, by the public methods of DuckDBConnection and of
// DuckDBConnectionTableFunctionExtensions. DuckDB.ExtensionKit compiles this folder too (see AGENTS.md here), so
// nothing in it depends on the ADO.NET classes: it takes the native connection, and what only DuckDB.NET does with
// the exception of a function goes through UdfErrors.
internal static class TableFunctionRegistration
{
    // The low-level registration: the rows are an IEnumerable, and the mapper writes a row as an object.
    public static void Register(DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, DuckDBLogicalType[] positionalLogicalTypes, NamedParameterDefinition[] namedParameters)
    {
        Register(connection, name, resultCallback, (data, projected, columnCount) => new ObjectTableFunctionScan(data.GetEnumerator(), mapperCallback, projected, columnCount), positionalLogicalTypes, namedParameters);
    }

    public static unsafe void Register(DuckDBNativeConnection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, TableFunctionScanFactory createScan, DuckDBLogicalType[] positionalLogicalTypes, NamedParameterDefinition[] namedParameters)
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

        var state = NativeMethods.TableFunction.DuckDBRegisterTableFunction(connection, function);

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

            // When the function gives no number of its own and its data is a collection, DuckDB is told the
            // collection's count. The length of an array cannot change before the scan reads it, so it is exact,
            // which DuckDB also takes as the most rows there are. Any other collection can be longer when it is
            // read than now, so its count is only an estimate, which DuckDB uses for planning only.
            var cardinality = tableFunctionData.Cardinality ?? tableFunctionData.Data switch
            {
                Array array => new CardinalityHint((ulong)array.LongLength, IsExact: true),
                ICollection collection => new CardinalityHint((ulong)collection.Count),
                IReadOnlyCollection<object> collection => new CardinalityHint((ulong)collection.Count),
                _ => null
            };

            if (cardinality is not null)
            {
                NativeMethods.TableFunction.DuckDBBindSetCardinality(info, cardinality.Value, isExact: cardinality.IsExact);
            }

            var connectionId = UdfErrors.TableFunctionBound(info);
            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data, tableFunctionData.DataFactory, connectionId);

            NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            UdfErrors.TableFunctionBindFailed(info, ex);

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
            var scan = rows is null ? null : functionInfo.CreateScan(rows, projected, bindData.Columns.Count);

            var logicalTypes = new DuckDBLogicalType[count];
            for (var i = 0; i < count; i++)
            {
                logicalTypes[i] = bindData.Columns[projected[i]].Type.GetLogicalType();
            }

            var initData = new TableFunctionInitData(scan, logicalTypes);
            NativeMethods.TableFunction.DuckDBInitSetInitData(info, initData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            if (bindData is not null)
            {
                UdfErrors.TableFunctionFailed(bindData.ConnectionId, ex);
            }
            NativeMethods.TableFunction.DuckDBInitSetError(info, ex.Message);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void TableFunction(IntPtr info, IntPtr chunk)
    {
        VectorDataWriterBase?[] writers = [];
        try
        {
            var initHandle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetInitData(info));

            if (initHandle.Target is not TableFunctionInitData tableFunctionInitData)
            {
                throw new InvalidOperationException("User defined table function failed. Function init data is null");
            }

            var dataChunk = new DuckDBDataChunk(chunk);
            var logicalTypes = tableFunctionInitData.LogicalTypes;

            writers = tableFunctionInitData.Writers;

            for (var columnIndex = 0; columnIndex < logicalTypes.Length; columnIndex++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, columnIndex);

                writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, logicalTypes[columnIndex]);
            }

            if (tableFunctionInitData.Scan is not { } scan)
            {
                throw new InvalidOperationException("User defined table function failed. No data source available");
            }

            var size = scan.Fill(tableFunctionInitData.Writers, DuckDBGlobalData.VectorSize);

            NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
        }
        catch (Exception ex)
        {
            try
            {
                var bindData = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetBindData(info));
                if (bindData.Target is TableFunctionBindData tableFunctionBindData)
                {
                    UdfErrors.TableFunctionFailed(tableFunctionBindData.ConnectionId, ex);
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
            // The writers of a chunk are not used for the next one. A slot is cleared, so that a chunk that fails
            // before it has made all its writers does not dispose one of the chunk before again.
            for (var index = 0; index < writers.Length; index++)
            {
                writers[index]?.Dispose();
                writers[index] = null;
            }
        }
    }

    private class NullValueReader : IDuckDBValueReader
    {
        public static readonly NullValueReader Instance = new();
        public bool IsNull() => true;
        public T GetValue<T>() => throw new InvalidOperationException("Cannot read value from a null parameter.");
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void DestroyExtraInfo(IntPtr pointer) => pointer.FreeHandle();
}

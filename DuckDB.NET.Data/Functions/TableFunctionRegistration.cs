using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Data.PreparedStatement;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Functions;

// Every table function is registered here, by the public methods of DuckDBConnection and of
// DuckDBConnectionTableFunctionExtensions. Nothing here depends on the ADO.NET classes: it takes the native V2
// connection, and what only DuckDB.NET does with the exception of a function goes through UdfErrors.
internal static class TableFunctionRegistration
{
    // The positional types of a table function, made when it is registered, from the factory of its connection.
    public static Func<IntPtr, DuckDBV2LogicalType>[] ForClrTypes(IReadOnlyList<Type> types)
    {
        var result = new Func<IntPtr, DuckDBV2LogicalType>[types.Count];
        for (var index = 0; index < result.Length; index++)
        {
            var type = types[index];
            result[index] = factory => DuckDBV2Types.ForClrType(factory, type);
        }

        return result;
    }

    public static void Register(DuckDBV2Connection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, Func<IntPtr, DuckDBV2LogicalType>[] positionalTypes, NamedParameterDefinition[] namedParameters)
    {
        Register(connection, name, resultCallback, (data, projected, columnCount) => new ObjectTableFunctionScan(data.GetEnumerator(), mapperCallback, projected, columnCount), positionalTypes, namedParameters);
    }

    // Every table function is registered here. Positional parameters are positional-only and required. Named ones are
    // named-only, and required unless they have a default (docs/V2-MIGRATION.md, "Named parameters and defaults").
    public static unsafe void Register(DuckDBV2Connection connection, string name, Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> resultCallback, TableFunctionScanFactory createScan, Func<IntPtr, DuckDBV2LogicalType>[] positionalTypes, NamedParameterDefinition[] namedParameters)
    {
        using var connectionLease = connection.Acquire();
        _duckdb_v2_error_info* error = null;

        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(connectionLease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        var info = new TableFunctionInfo(resultCallback, createScan, positionalTypes.Length, Array.ConvertAll(namedParameters, p => p.Name));
        _duckdb_v2_table_function* function = null;

        try
        {
            NativeMethodsV2.duckdb_v2_table_function_create(factory, &function, &error).ThrowIfFailed(ref error);

            using (var text = DuckDBV2Utf8.FromManaged(name))
            fixed (byte* pointer = text.Bytes)
            {
                NativeMethodsV2.duckdb_v2_table_function_set_name(function, (sbyte*)pointer, (ulong)text.Length, &error).ThrowIfFailed(ref error);
            }

            _duckdb_v2_function_signature* signature;
            NativeMethodsV2.duckdb_v2_table_function_get_signature(function, &signature, &error).ThrowIfFailed(ref error);

            for (var index = 0; index < positionalTypes.Length; index++)
            {
                using var type = positionalTypes[index]((IntPtr)factory);
                AddParameter(signature, $"arg{index}", type, null, DUCKDB_V2_FUNCTION_PARAMETER_KIND.POSITIONAL_ONLY);
            }

            foreach (var parameter in namedParameters)
            {
                using var type = DuckDBV2Types.ForClrType((IntPtr)factory, parameter.Type);
                using var defaultValue = parameter.HasDefault ? CreateDefault((IntPtr)factory, name, parameter, type) : null;
                AddParameter(signature, parameter.Name, type, defaultValue, DUCKDB_V2_FUNCTION_PARAMETER_KIND.NAMED_ONLY);
            }

            NativeMethodsV2.duckdb_v2_table_function_set_bind_callback(function, &Bind, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_table_function_set_init_global_callback(function, &Init, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_table_function_set_exec_callback(function, &TableFunction, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_table_function_set_projection_pushdown(function, 1, &error).ThrowIfFailed(ref error);

            // From here DuckDB owns the info, and frees its handle with the function.
            var userData = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(info)), destroy = &DestroyHandle };
            var userDataCode = NativeMethodsV2.duckdb_v2_table_function_set_user_data(function, &userData, &error);
            if (userDataCode != DUCKDB_V2_ERROR.NONE)
            {
                GCHandle.FromIntPtr((IntPtr)userData.ptr).Free();
                userDataCode.ThrowIfFailed(ref error);
            }

            var code = NativeMethodsV2.duckdb_v2_connection_register_table_function(connectionLease.Pointer, function, &error);
            if (code != DUCKDB_V2_ERROR.NONE)
            {
                try
                {
                    code.ThrowIfFailed(ref error);
                }
                catch (DuckDBException exception)
                {
                    throw new InvalidOperationException($"Error registering user defined table function: {name}", exception);
                }
            }
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_table_function_destroy(&function);
        }
    }

    private static unsafe void AddParameter(_duckdb_v2_function_signature* signature, string name, DuckDBV2LogicalType type, DuckDBV2Value? defaultValue,
                                            DUCKDB_V2_FUNCTION_PARAMETER_KIND kind)
    {
        using var typeLease = type.Acquire();
        using var text = DuckDBV2Utf8.FromManaged(name);
        var valuePointer = defaultValue is null ? null : (_duckdb_v2_value*)defaultValue.DangerousGetHandle();
        _duckdb_v2_error_info* error = null;

        fixed (byte* pointer = text.Bytes)
        {
            NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)pointer, (ulong)text.Length, typeLease.Pointer, valuePointer, kind, &error)
                           .ThrowIfFailed(ref error);
        }
    }

    // The default of a named parameter, cast here to the parameter's type: DuckDB casts a default only when the function
    // is called, so one that does not cast would fail every call that leaves the parameter out. Text is cast by DuckDB;
    // any other constant is converted as a command parameter of that type would be.
    private static unsafe DuckDBV2Value CreateDefault(IntPtr factory, string functionName, NamedParameterDefinition parameter, DuckDBV2LogicalType type)
    {
        using var typeLease = type.Acquire();

        DuckDBV2Value source;
        try
        {
            source = parameter.Default switch
            {
                null => DuckDBV2Values.Null(factory),
                string text when parameter.Type.UnderlyingTypeOrSelf() != typeof(string) => DuckDBV2Values.Varchar(factory, text),
                var constant => constant.ToDuckDBValue(factory, (IntPtr)typeLease.Pointer, DuckDBV2LogicalTypes.GetId(typeLease.Pointer).ToDuckDBType(), DbType.Object),
            };
        }
        catch (Exception exception)
        {
            throw InvalidDefault(functionName, parameter, exception);
        }

        using (source)
        {
            using var sourceLease = source.Acquire();
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_value* cast;
            var code = NativeMethodsV2.duckdb_v2_value_cast((_duckdb_v2_factory*)factory, sourceLease.Pointer, typeLease.Pointer, &cast, &error);

            try
            {
                code.ThrowIfFailed(ref error);
            }
            catch (Exception exception)
            {
                throw InvalidDefault(functionName, parameter, exception);
            }

            return new DuckDBV2Value(cast);
        }
    }

    private static InvalidOperationException InvalidDefault(string functionName, NamedParameterDefinition parameter, Exception exception) =>
        new($"The default '{parameter.Default}' of named parameter '{parameter.Name}' of table function '{functionName}' is not a {parameter.Type.Name}: {exception.Message}", exception);

    // Binding runs on the thread of the statement's connection (DuckDBV2CallbackScope): the connection goes into the bind
    // data, for the init and exec callbacks, which run on DuckDB's worker threads (UdfErrors).
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void Bind(_duckdb_v2_function_bind_info* info, _duckdb_v2_table_function_bind_info* result, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        var connection = UdfErrors.TableFunctionBound();
        DuckDBV2ValueReader[] positional = [];
        Dictionary<string, IDuckDBValueReader> named = [];

        try
        {
            void* userData;
            Check(NativeMethodsV2.duckdb_v2_function_bind_get_user_data(info, &userData, null));

            if (userData == null || GCHandle.FromIntPtr((IntPtr)userData).Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
            }

            positional = new DuckDBV2ValueReader[functionInfo.PositionalCount];
            for (var index = 0; index < positional.Length; index++)
            {
                positional[index] = ReadArgument(info, (ulong)index);
            }

            // A named parameter is always found: the call passed it, or it has a default (a required one left out is
            // DuckDB's binder error, before this callback).
            foreach (var parameterName in functionInfo.NamedParameterNames)
            {
                using var text = DuckDBV2Utf8.FromManaged(parameterName);
                ulong index;
                bool found;

                fixed (byte* pointer = text.Bytes)
                {
                    Check(NativeMethodsV2.duckdb_v2_function_bind_get_arg_index(info, (sbyte*)pointer, (ulong)text.Length, &index, &found, null));
                }

                named[parameterName] = found ? ReadArgument(info, index) : NullValueReader.Instance;
            }

            var tableFunctionData = functionInfo.Bind(positional, named);

            _duckdb_v2_factory* factory;
            Check(NativeMethodsV2.duckdb_v2_context_get_factory(context, &factory, null));

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var type = DuckDBV2Types.ForClrType((IntPtr)factory, columnInfo.Type);
                using var typeLease = type.Acquire();
                using var text = DuckDBV2Utf8.FromManaged(columnInfo.Name);

                fixed (byte* pointer = text.Bytes)
                {
                    Check(NativeMethodsV2.duckdb_v2_table_function_bind_add_result_column(result, (sbyte*)pointer, (ulong)text.Length, typeLease.Pointer, null));
                }
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
                Check(NativeMethodsV2.duckdb_v2_table_function_bind_set_cardinality(result, cardinality.Value, cardinality.IsExact ? (byte)1 : (byte)0, null));
            }

            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data, tableFunctionData.DataFactory, connection);
            var opaque = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(bindData)), destroy = &FreeHandle };
            if (NativeMethodsV2.duckdb_v2_function_bind_set_bind_data(info, &opaque, null) != DUCKDB_V2_ERROR.NONE)
            {
                GCHandle.FromIntPtr((IntPtr)opaque.ptr).Free();
                throw new InvalidOperationException("User defined table function bind failed. The bind data was refused");
            }
        }
        catch (Exception ex)
        {
            ReportError(connection, *err, ex);
        }
        finally
        {
            foreach (var parameter in positional)
            {
                parameter?.Dispose();
            }

            foreach (var parameter in named.Values)
            {
                (parameter as IDisposable)?.Dispose();
            }
        }
    }

    private static unsafe DuckDBV2ValueReader ReadArgument(_duckdb_v2_function_bind_info* info, ulong index)
    {
        _duckdb_v2_value* value;
        Check(NativeMethodsV2.duckdb_v2_function_bind_get_arg_value(info, index, &value, null));
        return new DuckDBV2ValueReader(new DuckDBV2Value(value));
    }

    // Called for every scan, which is the global state: DuckDB scans the function on one thread unless the function
    // says otherwise, so one scan state serves all its chunks.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void Init(_duckdb_v2_table_function_init_global_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        TableFunctionBindData? bindData = null;
        try
        {
            void* data;
            Check(NativeMethodsV2.duckdb_v2_table_function_init_global_get_bind_data(info, &data, null));
            if (data == null || GCHandle.FromIntPtr((IntPtr)data).Target is not TableFunctionBindData boundData)
            {
                throw new InvalidOperationException("User defined table function init failed. Bind data is null");
            }
            bindData = boundData;

            ulong columnCount;
            Check(NativeMethodsV2.duckdb_v2_table_function_init_global_get_column_count(info, &columnCount, null));
            var count = (int)columnCount;
            var projected = new int[count];
            for (var i = 0; i < count; i++)
            {
                ulong columnIndex;
                Check(NativeMethodsV2.duckdb_v2_table_function_init_global_get_column_index(info, (ulong)i, &columnIndex, null));
                projected[i] = (int)columnIndex;
            }

            void* userData;
            Check(NativeMethodsV2.duckdb_v2_table_function_init_global_get_user_data(info, &userData, null));
            if (userData == null || GCHandle.FromIntPtr((IntPtr)userData).Target is not TableFunctionInfo functionInfo)
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

            _duckdb_v2_factory* typeFactory;
            Check(NativeMethodsV2.duckdb_v2_context_get_factory(context, &typeFactory, null));

            var logicalTypes = new DuckDBV2LogicalType[count];
            TableFunctionInitData initData;
            try
            {
                for (var i = 0; i < count; i++)
                {
                    logicalTypes[i] = DuckDBV2Types.ForClrType((IntPtr)typeFactory, bindData.Columns[projected[i]].Type);
                }

                // The scan is disposed in TableFunctionInitData.Dispose.
                var scan = rows is null ? null : functionInfo.CreateScan(rows, projected, bindData.Columns.Count);
                initData = new TableFunctionInitData(scan, logicalTypes);
            }
            catch
            {
                foreach (var type in logicalTypes)
                {
                    type?.Dispose();
                }

                throw;
            }

            var state = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(initData)), destroy = &DestroyHandle };
            if (NativeMethodsV2.duckdb_v2_table_function_init_global_set_global_state(info, &state, null) != DUCKDB_V2_ERROR.NONE)
            {
                ((IntPtr)state.ptr).FreeHandle();
                throw new InvalidOperationException("User defined table function init failed. The scan state was refused");
            }
        }
        catch (Exception ex)
        {
            ReportError(bindData?.Connection, *err, ex);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void TableFunction(_duckdb_v2_table_function_exec_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        VectorDataWriterBase?[] writers = [];
        try
        {
            void* state;
            Check(NativeMethodsV2.duckdb_v2_table_function_exec_get_global_state(info, &state, null));

            if (state == null || GCHandle.FromIntPtr((IntPtr)state).Target is not TableFunctionInitData tableFunctionInitData)
            {
                throw new InvalidOperationException("User defined table function failed. Function init data is null");
            }

            _duckdb_v2_data_chunk* chunk;
            Check(NativeMethodsV2.duckdb_v2_table_function_exec_get_output_chunk(info, &chunk, null));

            var logicalTypes = tableFunctionInitData.LogicalTypes;
            writers = tableFunctionInitData.Writers;

            for (var columnIndex = 0; columnIndex < logicalTypes.Length; columnIndex++)
            {
                _duckdb_v2_vector* vector;
                Check(NativeMethodsV2.duckdb_v2_data_chunk_get_vector(chunk, (ulong)columnIndex, &vector, null));

                using var typeLease = logicalTypes[columnIndex].Acquire();
                writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, typeLease.Pointer);
            }

            if (tableFunctionInitData.Scan is not { } scan)
            {
                throw new InvalidOperationException("User defined table function failed. No data source available");
            }

            ulong capacity;
            Check(NativeMethodsV2.duckdb_v2_data_chunk_get_capacity(chunk, &capacity, null));
            var size = scan.Fill(tableFunctionInitData.Writers, capacity);

            // The size of the batch is set on its first vector; DuckDB takes it for the whole chunk. A scan whose query
            // reads no column has no vector.
            if (logicalTypes.Length > 0)
            {
                _duckdb_v2_vector* first;
                Check(NativeMethodsV2.duckdb_v2_data_chunk_get_vector(chunk, 0, &first, null));
                Check(NativeMethodsV2.duckdb_v2_vector_set_size(first, size, null));
            }
        }
        catch (Exception ex)
        {
            TableFunctionBindData? bindData = null;
            try
            {
                void* data;
                if (NativeMethodsV2.duckdb_v2_table_function_exec_get_bind_data(info, &data, null) == DUCKDB_V2_ERROR.NONE && data != null)
                {
                    bindData = GCHandle.FromIntPtr((IntPtr)data).Target as TableFunctionBindData;
                }
            }
            catch
            {
                // Without the connection we still report the error message
            }

            ReportError(bindData?.Connection, *err, ex);
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

    private static void Check(DUCKDB_V2_ERROR code)
    {
        if (code != DUCKDB_V2_ERROR.NONE)
        {
            throw new InvalidOperationException($"User defined table function failed: {code}");
        }
    }

    // The exception goes to UdfErrors, with the connection the function was bound on; DuckDB gets the message.
    private static unsafe void ReportError(DuckDBV2Connection? connection, _duckdb_v2_error_info* err, Exception exception)
    {
        UdfErrors.TableFunctionFailed(connection, exception);

        ScalarFunctionRegistration.SetError(err, exception.Message);
    }

    private class NullValueReader : IDuckDBValueReader
    {
        public static readonly NullValueReader Instance = new();
        public bool IsNull() => true;
        public T GetValue<T>() => throw new InvalidOperationException("Cannot read value from a null parameter.");
    }

    // The function's info and the scan state, disposed with them.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void DestroyHandle(void* pointer) => ((IntPtr)pointer).FreeHandle();

    // The bind data, which only the handle is freed for.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void FreeHandle(void* pointer) => GCHandle.FromIntPtr((IntPtr)pointer).Free();
}

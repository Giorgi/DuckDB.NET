using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.C;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// The internal table function that <see cref="DuckDBArrowAppender"/> inserts through: each batch is one
/// <c>INSERT INTO table SELECT * FROM __duckdbnet_arrow_append(id)</c>, and the function hands DuckDB the batch of the
/// appender with that id, converted by V2's Arrow importer.
/// </summary>
/// <remarks>
/// The importer needs a context with an active transaction, which only callbacks are given, so the conversion runs in
/// the function: its bind creates the importer, once per appender and schema, and declares the columns from it; its
/// exec appends the batch (consumed, so the chunks reference the Arrow buffers) and references each chunk's vectors
/// into the output, so nothing is copied before DuckDB writes the table. DuckDB releases the batch before the INSERT
/// returns. The function is registered when DuckDB creates an instance (<see cref="Register"/>, from the init callback of
/// <c>instance_open_shared</c>): a table function registered twice is refused, and registering one later would be part
/// of the user's transaction.
/// </remarks>
internal static unsafe class DuckDBV2ArrowAppend
{
    public const string FunctionName = "__duckdbnet_arrow_append";

    // The rows of a produced chunk, at most: the capacity of the output chunk it is referenced into.
    private const ulong BatchSize = 2048;

    private static readonly ConcurrentDictionary<long, State> States = new();
    private static long lastId;

    /// <summary>What the function reads for one appender: the batch of the current INSERT and the importer.</summary>
    internal sealed class State(DuckDBV2Connection connection) : IDisposable
    {
        public long Id { get; } = Interlocked.Increment(ref lastId);

        public DuckDBV2Connection Connection { get; } = connection;

        // The batch of the INSERT that runs, set by the appender before it executes and cleared after.
        public RecordBatch? Batch;

        private _duckdb_v2_arrow_importer* importer;
        private Schema? importerSchema;
        private ulong columnCount;
        private bool appended;
        private _duckdb_v2_data_chunk* pending;

        // Bind: the importer for the batch's schema. Batches of one stream share a Schema instance, so a stream
        // creates one importer; a batch with another Schema object gets a new one.
        public void Bind(_duckdb_v2_context* context, _duckdb_v2_table_function_bind_info* result)
        {
            var batch = Batch ?? throw new InvalidOperationException("The Arrow appender has no batch to insert.");

            if (importer == null || !ReferenceEquals(batch.Schema, importerSchema))
            {
                DestroyImporter();

                var cSchema = CArrowSchema.Create();
                try
                {
                    CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

                    _duckdb_v2_error_info* error = null;
                    _duckdb_v2_arrow_importer* created;
                    // The schema is read, not consumed: it is released here either way.
                    NativeMethodsV2.duckdb_v2_arrow_importer_create(context, (ArrowSchema*)cSchema, BatchSize, &created, &error)
                                   .ThrowIfFailed(ref error, "Failed to convert the Arrow schema to a DuckDB schema.");
                    importer = created;
                    importerSchema = batch.Schema;
                }
                finally
                {
                    CArrowSchema.Free(cSchema);
                }
            }

            _duckdb_v2_error_info* schemaError = null;
            _duckdb_v2_schema* schema;
            NativeMethodsV2.duckdb_v2_arrow_importer_get_schema(importer, &schema, &schemaError).ThrowIfFailed(ref schemaError);

            try
            {
                _duckdb_v2_error_info* error = null;
                ulong count;
                NativeMethodsV2.duckdb_v2_schema_get_count(schema, &count, &error).ThrowIfFailed(ref error);

                for (ulong index = 0; index < count; index++)
                {
                    sbyte* name;
                    ulong nameLength;
                    _duckdb_v2_logical_type* type;
                    NativeMethodsV2.duckdb_v2_schema_get_field(schema, index, &name, &nameLength, &type, &error).ThrowIfFailed(ref error);
                    NativeMethodsV2.duckdb_v2_table_function_bind_add_result_column(result, name, nameLength, type, &error).ThrowIfFailed(ref error);
                }

                columnCount = count;
            }
            finally
            {
                NativeMethodsV2.duckdb_v2_schema_destroy(&schema);
            }

            appended = false;
        }

        // Exec: the next chunk of the batch, its vectors referenced into the output; an empty output once it is read.
        public void Exec(_duckdb_v2_table_function_exec_info* info)
        {
            // The chunk referenced into the previous output is not read any more.
            DestroyPending();

            _duckdb_v2_error_info* error = null;

            if (!appended)
            {
                appended = true;
                var batch = Batch ?? throw new InvalidOperationException("The Arrow appender has no batch to insert.");
                var cArray = CArrowArray.Create();

                try
                {
                    CArrowArrayExporter.ExportRecordBatch(batch, cArray);

                    // Consumed: DuckDB takes the array (its release is set to NULL here) and releases it, which unpins the
                    // batch's buffers, once the chunks that reference them are gone. Flushed: no rows are held back.
                    NativeMethodsV2.duckdb_v2_arrow_importer_append(importer, (ArrowArray*)cArray, 1, 1, &error)
                                   .ThrowIfFailed(ref error, "Failed to convert an Arrow record batch to a DuckDB data chunk.");
                }
                finally
                {
                    // Releases the array only if DuckDB did not take it, and frees the struct either way.
                    CArrowArray.Free(cArray);
                }
            }

            _duckdb_v2_data_chunk* chunk;
            NativeMethodsV2.duckdb_v2_arrow_importer_next_chunk(importer, &chunk, &error)
                           .ThrowIfFailed(ref error, "Failed to convert an Arrow record batch to a DuckDB data chunk.");

            _duckdb_v2_data_chunk* output;
            NativeMethodsV2.duckdb_v2_table_function_exec_get_output_chunk(info, &output, &error).ThrowIfFailed(ref error);

            ulong size = 0;
            if (chunk != null)
            {
                pending = chunk;
                NativeMethodsV2.duckdb_v2_data_chunk_get_size(chunk, &size, &error).ThrowIfFailed(ref error);

                for (ulong index = 0; index < columnCount; index++)
                {
                    _duckdb_v2_vector* source, target;
                    NativeMethodsV2.duckdb_v2_data_chunk_get_vector(chunk, index, &source, &error).ThrowIfFailed(ref error);
                    NativeMethodsV2.duckdb_v2_data_chunk_get_vector(output, index, &target, &error).ThrowIfFailed(ref error);
                    NativeMethodsV2.duckdb_v2_vector_reference(target, source, &error).ThrowIfFailed(ref error);
                }
            }

            // The size of the batch is set on its first vector; DuckDB takes it for the whole chunk.
            if (columnCount > 0)
            {
                _duckdb_v2_vector* first;
                NativeMethodsV2.duckdb_v2_data_chunk_get_vector(output, 0, &first, &error).ThrowIfFailed(ref error);
                NativeMethodsV2.duckdb_v2_vector_set_size(first, size, &error).ThrowIfFailed(ref error);
            }
        }

        /// <summary>
        /// After the INSERT: the last chunk goes, and after a failure the importer too, which releases a batch DuckDB
        /// had not read to its end.
        /// </summary>
        public void EndBatch(bool failed)
        {
            Batch = null;
            DestroyPending();

            if (failed)
            {
                DestroyImporter();
            }
        }

        public void Dispose()
        {
            States.TryRemove(Id, out _);
            Batch = null;
            DestroyPending();
            DestroyImporter();
        }

        private void DestroyPending()
        {
            if (pending != null)
            {
                fixed (_duckdb_v2_data_chunk** slot = &pending)
                {
                    NativeMethodsV2.duckdb_v2_data_chunk_destroy(slot);
                }
            }
        }

        private void DestroyImporter()
        {
            if (importer != null)
            {
                fixed (_duckdb_v2_arrow_importer** slot = &importer)
                {
                    NativeMethodsV2.duckdb_v2_arrow_importer_destroy(slot);
                }
            }

            importerSchema = null;
        }
    }

    /// <summary>A state for an appender on <paramref name="connection"/>, found by the function through its id.</summary>
    public static State CreateState(DuckDBV2Connection connection)
    {
        var state = new State(connection);
        States[state.Id] = state;
        return state;
    }

    /// <summary>
    /// Registers the function on <paramref name="instance"/>'s database, through a connection of its own. Called once
    /// for every instance DuckDB creates, before any other open can share it, so the function is registered once and
    /// outside any transaction of the user's. A failure is left unreported: the database opens, and only the Arrow
    /// appender fails, when its INSERT cannot find the function.
    /// </summary>
    public static void Register(_duckdb_v2_instance* instance)
    {
        _duckdb_v2_connection* connection = null;
        _duckdb_v2_table_function* function = null;

        try
        {
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_connection_create(instance, &connection, &error).ThrowIfFailed(ref error);

            _duckdb_v2_factory* factory;
            NativeMethodsV2.duckdb_v2_connection_get_factory(connection, &factory, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_table_function_create(factory, &function, &error).ThrowIfFailed(ref error);

            using (var name = DuckDBV2Utf8.FromManaged(FunctionName))
            fixed (byte* pointer = name.Bytes)
            {
                NativeMethodsV2.duckdb_v2_table_function_set_name(function, (sbyte*)pointer, (ulong)name.Length, &error).ThrowIfFailed(ref error);
            }

            _duckdb_v2_function_signature* signature;
            NativeMethodsV2.duckdb_v2_table_function_get_signature(function, &signature, &error).ThrowIfFailed(ref error);

            using (var bigint = DuckDBV2Types.FromId((IntPtr)factory, DUCKDB_V2_LOGICAL_TYPE_ID.BIGINT))
            using (var typeLease = bigint.Acquire())
            using (var parameter = DuckDBV2Utf8.FromManaged("id"))
            fixed (byte* pointer = parameter.Bytes)
            {
                NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)pointer, (ulong)parameter.Length, typeLease.Pointer, null,
                                                                           DUCKDB_V2_FUNCTION_PARAMETER_KIND.POSITIONAL_ONLY, &error).ThrowIfFailed(ref error);
            }

            NativeMethodsV2.duckdb_v2_table_function_set_bind_callback(function, &Bind, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_table_function_set_exec_callback(function, &Exec, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_connection_register_table_function(connection, function, &error).ThrowIfFailed(ref error);
        }
        catch
        {
            // See the summary: the open goes on without the function.
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_table_function_destroy(&function);
            NativeMethodsV2.duckdb_v2_connection_destroy(&connection);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Bind(_duckdb_v2_function_bind_info* info, _duckdb_v2_table_function_bind_info* result, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        State? state = null;

        try
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_value* value;
            NativeMethodsV2.duckdb_v2_function_bind_get_arg_value(info, 0, &value, &error).ThrowIfFailed(ref error);

            long id;
            try
            {
                NativeMethodsV2.duckdb_v2_value_get_bigint(value, &id, &error).ThrowIfFailed(ref error);
            }
            finally
            {
                NativeMethodsV2.duckdb_v2_value_destroy(&value);
            }

            // An id that is no appender's: the function was called from SQL.
            if (!States.TryGetValue(id, out state))
            {
                throw new InvalidOperationException($"{FunctionName} is internal to DuckDB.NET's Arrow appender.");
            }

            state.Bind(context, result);

            // The id is the bind data, for the exec callback; it is not an allocation, so there is nothing to destroy.
            var bindData = new duckdb_v2_opaque { ptr = (void*)id };
            NativeMethodsV2.duckdb_v2_function_bind_set_bind_data(info, &bindData, &error).ThrowIfFailed(ref error);
        }
        catch (Exception exception)
        {
            ReportError(state?.Connection ?? DuckDBV2CallbackScope.Current, *err, exception);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Exec(_duckdb_v2_table_function_exec_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        State? state = null;

        try
        {
            _duckdb_v2_error_info* error = null;
            void* bindData;
            NativeMethodsV2.duckdb_v2_table_function_exec_get_bind_data(info, &bindData, &error).ThrowIfFailed(ref error);

            if (!States.TryGetValue((long)bindData, out state))
            {
                throw new InvalidOperationException("The Arrow appender was closed while its batch was inserted.");
            }

            state.Exec(info);
        }
        catch (Exception exception)
        {
            ReportError(state?.Connection, *err, exception);
        }
    }

    // The exception goes to the connection, which attaches it to its command's DuckDBException; DuckDB gets the message.
    private static void ReportError(DuckDBV2Connection? connection, _duckdb_v2_error_info* err, Exception exception)
    {
        if (connection is not null)
        {
            connection.CallbackException = exception;
        }

        // DuckDB puts the error type before the text it is given, so a DuckDB error passed on keeps its type only once.
        const string inputError = "Invalid Input Error: ";
        var message = exception is DuckDBException && exception.Message.StartsWith(inputError, StringComparison.Ordinal)
            ? exception.Message[inputError.Length..]
            : exception.Message;

        using var text = DuckDBV2Utf8.FromManaged(message);

        fixed (byte* pointer = text.Bytes)
        {
            NativeMethodsV2.duckdb_v2_error_info_set_code(err, DUCKDB_V2_ERROR.INPUT_INVALID);
            NativeMethodsV2.duckdb_v2_error_info_set_text(err, (sbyte*)pointer, (ulong)text.Length);
        }
    }
}

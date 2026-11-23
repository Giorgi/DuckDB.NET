using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using DuckDB.NET.Data.PreparedStatement;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// What binding needs to know about a statement's parameters, read once from <c>statement_bind</c>'s parameter schema:
/// each parameter's name, which is also its binding key ("1", "2", ... for a positional parameter, the identifier for
/// a named one), its logical type, and its type id. An untyped parameter (<c>SELECT $1</c>) reports UNKNOWN, which is
/// <see cref="DuckDBType.Invalid"/> here, as V1 reported it. The names are kept as UTF-8 in native memory, so an
/// execution passes them without encoding them again.
/// </summary>
internal sealed unsafe class DuckDBV2Parameters : IDisposable
{
    private readonly DuckDBV2LogicalType[] types;
    private readonly DuckDBType[] typeIds;
    private readonly string[] names;
    private readonly int[] positions;
    private readonly Dictionary<string, int> indexesByName;
    private readonly sbyte** namePointers;
    private readonly ulong* nameLengths;
    private readonly byte* nameBytes;
    private int disposed;

    private DuckDBV2Parameters(DuckDBV2StatementResult.Schema fields)
    {
        Count = fields.Count;
        types = new DuckDBV2LogicalType[Count];
        typeIds = new DuckDBType[Count];
        names = new string[Count];
        positions = new int[Count];
        indexesByName = new Dictionary<string, int>(Count, StringComparer.OrdinalIgnoreCase);

        var total = 0;

        for (var index = 0; index < Count; index++)
        {
            var (name, type) = fields.GetField(index);

            types[index] = DuckDBV2LogicalTypes.Copy((_duckdb_v2_logical_type*)type);
            typeIds[index] = DuckDBV2LogicalTypes.GetId((_duckdb_v2_logical_type*)type).ToDuckDBType();
            names[index] = name;

            // A positional parameter is named after its number; $1 and $3 without $2 are "1" and "3".
            positions[index] = int.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out var position) ? position : index + 1;
            indexesByName.TryAdd(name, index);
            total += Encoding.UTF8.GetByteCount(name);
        }

        namePointers = (sbyte**)NativeMemory.Alloc((nuint)Math.Max(Count, 1), (nuint)sizeof(sbyte*));
        nameLengths = (ulong*)NativeMemory.Alloc((nuint)Math.Max(Count, 1), sizeof(ulong));
        nameBytes = (byte*)NativeMemory.Alloc((nuint)Math.Max(total, 1));

        var offset = 0;
        for (var index = 0; index < Count; index++)
        {
            var length = Encoding.UTF8.GetBytes(names[index], new Span<byte>(nameBytes + offset, total - offset));
            namePointers[index] = (sbyte*)(nameBytes + offset);
            nameLengths[index] = (ulong)length;
            offset += length;
        }
    }

    public int Count { get; }

    /// <summary>The parameters of a parsed statement, bound without running it.</summary>
    public static DuckDBV2Parameters Read(DuckDBV2Connection connection, DuckDBV2SqlStatement statement)
    {
        using var connectionLease = connection.Acquire();
        using var statementLease = statement.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_schema* output;
        _duckdb_v2_schema* parameters;
        NativeMethodsV2.duckdb_v2_statement_bind(connectionLease.Pointer, statementLease.Pointer, &output, &parameters, &error)
                       .ThrowIfFailed(ref error, "Binding the statement failed");

        NativeMethodsV2.duckdb_v2_schema_destroy(&output);

        using var schema = new DuckDBV2StatementResult.Schema(parameters);
        return new DuckDBV2Parameters(schema);
    }

    // The result is a _duckdb_v2_result* or, for an Arrow execution, a _duckdb_v2_arrow_result*.
    public delegate DUCKDB_V2_ERROR ExecuteFunction(sbyte** names, ulong* nameLengths, _duckdb_v2_value** values, ulong count,
                                                    void** result, _duckdb_v2_error_info** error);

    /// <summary>
    /// Converts the collection's values to the parameters and runs <paramref name="execute"/> with them, as V1 bound
    /// them:
    /// <list type="bullet">
    /// <item>Fewer values than the statement needs is an <see cref="InvalidOperationException"/>; more is allowed.</item>
    /// <item>When any parameter in the collection has a name, every one is bound by name, and a name the statement does
    /// not declare is skipped. Otherwise they are bound by position, $1 to the first.</item>
    /// </list>
    /// Every value is passed with its parameter's name, which is its binding key either way.
    /// </summary>
    public DUCKDB_V2_ERROR Execute(DuckDBV2Connection connection, DuckDBParameterCollection parameters, ExecuteFunction execute,
                                   void** result, _duckdb_v2_error_info** error)
    {
        var count = parameters.Count;

        var hasNamedParameters = false;
        for (var i = 0; i < count; i++)
        {
            if (!string.IsNullOrEmpty(parameters[i].ParameterName))
            {
                hasNamedParameters = true;
                break;
            }
        }

        var expected = Count;
        if (!hasNamedParameters)
        {
            for (var index = 0; index < Count; index++)
            {
                expected = Math.Max(expected, positions[index]);
            }
        }

        if (count < expected)
        {
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expected}, got {count}");
        }

        // Which collection parameter each statement parameter takes, or null when the collection has none for it.
        var selected = new DuckDBParameter?[Count];

        if (hasNamedParameters)
        {
            for (var i = 0; i < count; i++)
            {
                var parameter = parameters[i];
                if (parameter.ParameterName is { Length: > 0 } name && indexesByName.TryGetValue(name, out var index))
                {
                    selected[index] = parameter;
                }
            }
        }
        else
        {
            for (var index = 0; index < Count; index++)
            {
                selected[index] = parameters[positions[index] - 1];
            }
        }

        var factory = GetFactory(connection);
        var values = new DuckDBV2Value?[Count];
        var valuePointers = stackalloc IntPtr[Math.Max(Count, 1)];
        var keyPointers = stackalloc IntPtr[Math.Max(Count, 1)];
        var keyLengths = stackalloc ulong[Math.Max(Count, 1)];
        var bound = 0;

        try
        {
            for (var index = 0; index < Count; index++)
            {
                if (selected[index] is not { } parameter)
                {
                    continue;
                }

                using var typeLease = types[index].Acquire();
                var label = new ParameterLabel(positions[index], hasNamedParameters ? parameter.ParameterName : null);
                var value = parameter.Value.ToDuckDBValue(factory, (IntPtr)typeLease.Pointer, typeIds[index], parameter.AssignedDbType, label);

                values[index] = value;
                valuePointers[bound] = value.DangerousGetHandle();
                keyPointers[bound] = (IntPtr)namePointers[index];
                keyLengths[bound] = nameLengths[index];
                bound++;
            }

            return execute((sbyte**)keyPointers, keyLengths, (_duckdb_v2_value**)valuePointers, (ulong)bound, result, error);
        }
        finally
        {
            // Executing copies the values in, so they are released here.
            foreach (var value in values)
            {
                value?.Dispose();
            }
        }
    }

    // Borrowed from the connection: values made with it see the catalog's types and the session's settings.
    private static IntPtr GetFactory(DuckDBV2Connection connection)
    {
        using var lease = connection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);
        return (IntPtr)factory;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        foreach (var type in types)
        {
            type?.Dispose();
        }

        NativeMemory.Free(namePointers);
        NativeMemory.Free(nameLengths);
        NativeMemory.Free(nameBytes);
    }
}

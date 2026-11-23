using System.Runtime.InteropServices;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// Probes for the design of named parameters (docs/V2-MIGRATION.md, "Named parameters and defaults"): what DuckDB does
// with a named parameter that has no default, a NULL default and a VARCHAR default, and what reflection reports for
// the default values of a lambda.
public unsafe class V2NamedParameterProbeTests
{
    [Fact]
    public void Probe()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        Register(connection, "f_required", "INTEGER", null);
        Report(connection, "SELECT f_required(1)");
        Report(connection, "SELECT f_required(1, n := 5)");
        Report(connection, "SELECT f_required(1, n := NULL)");
        Report(connection, "SELECT f_required(1, N := 6)");

        Register(connection, "f_null", "INTEGER", factory => DuckDBV2Values.Null(factory));
        Report(connection, "SELECT f_null(1) IS NULL");

        Register(connection, "f_date", "DATE", factory => DuckDBV2Values.Varchar(factory, "2024-01-01"));
        Report(connection, "SELECT f_date(1)");

        Register(connection, "f_uuid", "UUID", factory => DuckDBV2Values.Varchar(factory, "00000000-0000-0000-0000-000000000001"));
        Report(connection, "SELECT f_uuid(1)");

        Register(connection, "f_list", "INTEGER[]", factory => DuckDBV2Values.Varchar(factory, "[1, 2, 3]"));
        Report(connection, "SELECT f_list(1)");

        try
        {
            Register(connection, "f_bad", "DATE", factory => DuckDBV2Values.Varchar(factory, "not a date"));
            Console.WriteLine("PROBE register f_bad: accepted");
            Report(connection, "SELECT f_bad(1)");
        }
        catch (Exception exception)
        {
            Console.WriteLine($"PROBE register f_bad: {exception.Message}");
        }

        Report(connection, "SELECT parameters, parameter_types FROM duckdb_functions() WHERE function_name = 'f_date'");

        // Reflection: a lambda with defaults, as a Func and as its natural delegate type.
        Func<int, string, int, string> asFunc = (int count, string prefix = "Mr.", int limit = 100) => prefix;
        var natural = (int count, string prefix = "Mr.", DateTime when = default, Guid id = default, string none = null) => prefix;
        foreach (var (label, method) in new[] { ("Func", asFunc.Method), ("natural", natural.Method) })
        {
            foreach (var parameter in method.GetParameters())
            {
                Console.WriteLine($"PROBE reflection {label} {parameter.Name}: HasDefaultValue={parameter.HasDefaultValue} DefaultValue={parameter.DefaultValue ?? "null"}");
            }
        }
    }

    private static void Report(DuckDBConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = command.ExecuteReader();
            var rows = new List<string>();
            while (reader.Read())
            {
                rows.Add(string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i) is System.Collections.IEnumerable e and not string ? "[" + string.Join(",", e.Cast<object>()) + "]" : reader.GetValue(i)))));
            }
            Console.WriteLine($"PROBE {sql} => {string.Join(" | ", rows)}");
        }
        catch (Exception exception)
        {
            Console.WriteLine($"PROBE {sql} => {exception.GetType().Name}: {exception.Message}");
        }
    }

    // name(x INTEGER, n <type> NAMED_ONLY [default]) returns n.
    private static void Register(DuckDBConnection connection, string name, string typeText, Func<IntPtr, DuckDBV2Value> defaultValue)
    {
        using var lease = connection.V2Connection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        _duckdb_v2_scalar_function* function;
        NativeMethodsV2.duckdb_v2_scalar_function_create(factory, &function, &error).ThrowIfFailed(ref error);
        using var value = defaultValue?.Invoke((IntPtr)factory);

        try
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* pointer = bytes)
            {
                NativeMethodsV2.duckdb_v2_scalar_function_set_name(function, (sbyte*)pointer, (ulong)bytes.Length, &error).ThrowIfFailed(ref error);
            }

            using var integer = DuckDBV2Types.FromId((IntPtr)factory, DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER);
            using var type = DuckDBV2Types.FromText((IntPtr)factory, typeText);
            using var integerLease = integer.Acquire();
            using var typeLease = type.Acquire();

            _duckdb_v2_function_signature* signature;
            NativeMethodsV2.duckdb_v2_scalar_function_get_signature(function, &signature, &error).ThrowIfFailed(ref error);
            var x = "x"u8.ToArray();
            var n = "n"u8.ToArray();
            fixed (byte* xPointer = x)
            fixed (byte* nPointer = n)
            {
                NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)xPointer, 1, integerLease.Pointer, null,
                                                                           DUCKDB_V2_FUNCTION_PARAMETER_KIND.POSITIONAL_ONLY, &error).ThrowIfFailed(ref error);

                var valuePointer = value is null ? null : (_duckdb_v2_value*)value.DangerousGetHandle();
                NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)nPointer, 1, typeLease.Pointer, valuePointer,
                                                                           DUCKDB_V2_FUNCTION_PARAMETER_KIND.NAMED_ONLY, &error).ThrowIfFailed(ref error);
            }

            NativeMethodsV2.duckdb_v2_function_signature_set_return_type(signature, typeLease.Pointer, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_scalar_function_set_exec_callback(function, &Exec, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_connection_register_scalar_function(lease.Pointer, function, &error).ThrowIfFailed(ref error);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_scalar_function_destroy(&function);
        }
    }

    // Returns argument 1, the named one.
    [UnmanagedCallersOnly(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    private static void Exec(_duckdb_v2_scalar_function_exec_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        ulong rows;
        _duckdb_v2_vector* input;
        _duckdb_v2_vector* output;
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_row_count(info, &rows, null);
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_arg(info, 1, &input, null);
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_result(info, &output, null);

        _duckdb_v2_logical_type* type;
        NativeMethodsV2.duckdb_v2_vector_get_logical_type(input, &type, null);
        using var owned = new DuckDBV2LogicalType(type);
        using var lease = owned.Acquire();

        using var reader = VectorDataReaderFactory.CreateReader(lease.Pointer, "n");
        reader.Reset(input);
        using var writer = VectorDataWriterFactory.CreateWriter(output, lease.Pointer);

        for (ulong row = 0; row < rows; row++)
        {
            if (reader.IsValid(row))
            {
                writer.WriteValue(reader.GetValue(row), row);
            }
            else
            {
                writer.WriteNull(row);
            }
        }
    }
}

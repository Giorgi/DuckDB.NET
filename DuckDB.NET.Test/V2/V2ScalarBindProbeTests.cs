using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Threading;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// Stage 5 depends on a scalar function's bind callback running on the thread that executes the statement, so it can
// find the executing connection in a [ThreadStatic] and hand its exception slot to the exec callbacks as bind data.
// These tests check that with the raw V2 API before anything is built on it.
[Trait("V2", "Ready")]
public unsafe class V2ScalarBindProbeTests
{
    private sealed class Slot
    {
        public readonly ConcurrentBag<int> ExecThreads = [];
        public Exception Exception;
    }

    private sealed record BindCall(int Thread, Slot Slot);

    [ThreadStatic] private static Slot current;

    private static readonly ConcurrentQueue<BindCall> Binds = new();
    private static int destroyed;

    [Fact]
    public void BindRunsOnTheExecutingThreadForEveryPath()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:;threads=4");
        connection.Open();
        Register(connection, "bind_probe");

        connection.ExecuteNonQuery("CREATE TABLE t AS SELECT range::INTEGER AS i FROM range(2000000)");
        var me = Environment.CurrentManagedThreadId;

        // 1. An unprepared query whose execution is spread over the worker threads.
        var slot = new Slot();
        Run(slot, () => Scalar(connection, "SELECT sum(bind_probe(i))::BIGINT FROM t").Should().Be(Enumerable.Range(0, 2000000).Sum(i => (long)i)));
        AssertBound(slot, me);
        Console.WriteLine($"exec threads (unprepared): {slot.ExecThreads.Distinct().Count()}, any on caller: {slot.ExecThreads.Contains(me)}");

        // 2. A prepared statement, bound when it is prepared and executed three times.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT sum(bind_probe(i))::BIGINT FROM t WHERE i < $1";
        command.Parameters.Add(new DuckDBParameter(10));
        slot = new Slot();
        Run(slot, () => command.Prepare());
        Console.WriteLine($"binds at Prepare: {Binds.Count}");
        Binds.Should().OnlyContain(b => b.Thread == me && b.Slot == slot);

        for (var i = 0; i < 3; i++)
        {
            var execution = new Slot();
            Run(execution, () => command.ExecuteScalar().Should().Be(45L));
            Console.WriteLine($"binds at execution {i}: {Binds.Count}; exec saw this execution's slot: {!execution.ExecThreads.IsEmpty}, the Prepare's: {!slot.ExecThreads.IsEmpty}");
            Binds.Should().OnlyContain(b => b.Thread == me && b.Slot == execution);
        }

        // 3. The table changes under the prepared statement, which DuckDB rebinds on its next execution.
        connection.ExecuteNonQuery("CREATE OR REPLACE TABLE t AS SELECT range::INTEGER AS i FROM range(100)");
        slot = new Slot();
        Run(slot, () => command.ExecuteScalar().Should().Be(45L));
        Binds.Should().NotBeEmpty("the statement was rebound");
        Binds.Should().OnlyContain(b => b.Thread == me && b.Slot == slot);
    }

    [Fact]
    public void AnErrorReachesTheCommandAndTheExceptionTheSlot()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:;threads=4");
        connection.Open();
        Register(connection, "bind_probe");

        var slot = new Slot();
        var act = () => Run(slot, () => Scalar(connection, "SELECT sum(bind_probe(i::INTEGER)) FROM range(3000000) r(i)"));
        var thrown = act.Should().Throw<DuckDBException>().Which;
        Console.WriteLine($"error seen by the command: {thrown.Message}");

        thrown.Message.Should().Contain("probe refused 2999999");
        slot.Exception.Should().BeOfType<InvalidOperationException>().Which.Message.Should().Be("probe refused 2999999");
    }

    [Fact]
    public void BindDataIsDestroyed()
    {
        var before = Volatile.Read(ref destroyed);
        using (var connection = new DuckDBConnection("DataSource=:memory:"))
        {
            connection.Open();
            Register(connection, "bind_probe");
            for (var i = 0; i < 5; i++)
            {
                Run(new Slot(), () => Scalar(connection, "SELECT bind_probe(1)").Should().Be(1));
            }
        }

        Volatile.Read(ref destroyed).Should().BeGreaterThanOrEqualTo(before + 5);
    }

    private static object Scalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static void Run(Slot slot, Action action)
    {
        Binds.Clear();
        current = slot;
        try
        {
            action();
        }
        finally
        {
            current = null;
        }
    }

    private static void AssertBound(Slot slot, int thread)
    {
        Binds.Should().NotBeEmpty();
        Binds.Should().OnlyContain(b => b.Thread == thread && b.Slot == slot);
    }

    // probe(x INTEGER) -> INTEGER returns x, and fails on 2999999.
    private static void Register(DuckDBConnection connection, string name)
    {
        using var lease = connection.V2Connection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        _duckdb_v2_scalar_function* function;
        NativeMethodsV2.duckdb_v2_scalar_function_create(factory, &function, &error).ThrowIfFailed(ref error);

        try
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(name);
            fixed (byte* pointer = bytes)
            {
                NativeMethodsV2.duckdb_v2_scalar_function_set_name(function, (sbyte*)pointer, (ulong)bytes.Length, &error).ThrowIfFailed(ref error);
            }

            using var integer = DuckDBV2Types.FromId((IntPtr)factory, DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER);
            using var typeLease = integer.Acquire();

            _duckdb_v2_function_signature* signature;
            NativeMethodsV2.duckdb_v2_scalar_function_get_signature(function, &signature, &error).ThrowIfFailed(ref error);
            var x = "x"u8.ToArray();
            fixed (byte* pointer = x)
            {
                NativeMethodsV2.duckdb_v2_function_signature_add_parameter(signature, (sbyte*)pointer, 1, typeLease.Pointer, null,
                                                                           DUCKDB_V2_FUNCTION_PARAMETER_KIND.STANDARD, &error).ThrowIfFailed(ref error);
            }
            NativeMethodsV2.duckdb_v2_function_signature_set_return_type(signature, typeLease.Pointer, &error).ThrowIfFailed(ref error);

            NativeMethodsV2.duckdb_v2_scalar_function_set_bind_callback(function, &Bind, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_scalar_function_set_exec_callback(function, &Exec, &error).ThrowIfFailed(ref error);
            NativeMethodsV2.duckdb_v2_connection_register_scalar_function(lease.Pointer, function, &error).ThrowIfFailed(ref error);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_scalar_function_destroy(&function);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    private static void Bind(_duckdb_v2_function_bind_info* info, _duckdb_v2_scalar_function_bind_info* result, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        var slot = current;
        Binds.Enqueue(new BindCall(Environment.CurrentManagedThreadId, slot));

        if (slot is null)
        {
            return;
        }

        var data = new duckdb_v2_opaque
        {
            ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(slot)),
            destroy = &DestroyBindData,
        };
        NativeMethodsV2.duckdb_v2_scalar_function_bind_set_bind_data(result, &data, null);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    private static void DestroyBindData(void* data)
    {
        GCHandle.FromIntPtr((IntPtr)data).Free();
        Interlocked.Increment(ref destroyed);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(System.Runtime.CompilerServices.CallConvCdecl)])]
    private static void Exec(_duckdb_v2_scalar_function_exec_info* info, _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        void* data;
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_bind_data(info, &data, null);
        var slot = data == null ? null : (Slot)GCHandle.FromIntPtr((IntPtr)data).Target;
        slot?.ExecThreads.Add(Environment.CurrentManagedThreadId);

        ulong rows;
        _duckdb_v2_vector* input;
        _duckdb_v2_vector* output;
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_row_count(info, &rows, null);
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_arg(info, 0, &input, null);
        NativeMethodsV2.duckdb_v2_scalar_function_exec_get_result(info, &output, null);

        NativeMethodsV2.duckdb_v2_vector_flatten(input, null);
        var view = Data.DataChunk.Reader.DuckDBV2Vectors.ReadFlat(input);
        var source = (int*)view.data;
        var target = (int*)Data.DataChunk.Reader.DuckDBV2Vectors.GetMutableData(output);

        for (ulong row = 0; row < rows; row++)
        {
            if (source[row] == 2999999)
            {
                var exception = new InvalidOperationException("probe refused 2999999");
                if (slot is not null)
                {
                    slot.Exception = exception;
                }

                var text = System.Text.Encoding.UTF8.GetBytes(exception.Message);
                fixed (byte* pointer = text)
                {
                    NativeMethodsV2.duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR.INPUT_INVALID);
                    NativeMethodsV2.duckdb_v2_error_info_set_text(*err, (sbyte*)pointer, (ulong)text.Length);
                }
                return;
            }

            target[row] = source[row];
        }
    }
}

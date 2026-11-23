using System.Text;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Opens databases and connections through the C API V2: one environment for the process, an instance per database,
/// and connections on the instance.
/// </summary>
internal static unsafe class DuckDBV2Startup
{
    private const string InMemoryPath = ":memory:";

    // Created on first use and never destroyed: environment_destroy refuses while an instance is alive, and a static
    // field keeps the SafeHandle from being finalized.
    private static readonly Lazy<DuckDBV2Environment> SharedEnvironment = new(CreateEnvironment);

    internal static DuckDBV2Environment Environment => SharedEnvironment.Value;

    /// <summary>
    /// Creates an instance with the connection string's configuration as startup options, and attaches the database
    /// at <paramref name="path"/> (in memory when null) as its default database.
    /// </summary>
    public static DuckDBV2Instance OpenInstance(string? path, IReadOnlyDictionary<string, string> options)
    {
        DuckDBV2Instance instance;

        try
        {
            instance = CreateInstance(options);
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode is { } code)
        {
            throw NameTheRefusedOption(options, exception, code);
        }

        try
        {
            Attach(instance, path ?? InMemoryPath);
            return instance;
        }
        catch
        {
            instance.Dispose();
            throw;
        }
    }

    public static DuckDBV2Connection Connect(DuckDBV2Instance instance)
    {
        using var lease = instance.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_connection* connection;
        NativeMethodsV2.duckdb_v2_connection_create(lease.Pointer, &connection, &error).ThrowIfFailed(ref error, "Connecting to the database failed");

        return new DuckDBV2Connection(connection);
    }

    /// <summary>
    /// The progress of the query running on the connection. -1 when there is none, or when progress is not tracked
    /// (it needs <c>enable_progress_bar</c>).
    /// </summary>
    public static DuckDBQueryProgress QueryProgress(DuckDBV2Connection connection)
    {
        using var lease = connection.Acquire();

        _duckdb_v2_error_info* error = null;
        double percentage;
        ulong rowsProcessed;
        ulong totalRowsToProcess;
        NativeMethodsV2.duckdb_v2_connection_progress_get(lease.Pointer, &percentage, &rowsProcessed, &totalRowsToProcess, &error)
                       .ThrowIfFailed(ref error);

        return new DuckDBQueryProgress(percentage, rowsProcessed, totalRowsToProcess);
    }

    public static string LibraryVersion()
    {
        _duckdb_v2_error_info* error = null;
        sbyte* version;
        ulong length;
        NativeMethodsV2.duckdb_v2_library_version(&version, &length, &error).ThrowIfFailed(ref error);

        return DuckDBV2Utf8.ToManaged(version, length);
    }

    private static DuckDBV2Environment CreateEnvironment()
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_environment* environment;
        NativeMethodsV2.duckdb_v2_environment_create(&environment, &error).ThrowIfFailed(ref error, "Creating the DuckDB environment failed");

        return new DuckDBV2Environment(environment);
    }

    private static DuckDBV2Instance CreateInstance(IReadOnlyDictionary<string, string> options)
    {
        var count = options.Count;

        // All names and values in one buffer, each without a terminator, addressed by offset and length.
        var lengths = new ulong[count * 2];
        var offsets = new int[count * 2];
        var total = 0;
        var index = 0;

        foreach (var (name, value) in options)
        {
            foreach (var text in (ReadOnlySpan<string>)[name, value])
            {
                offsets[index] = total;
                lengths[index] = (ulong)Encoding.UTF8.GetByteCount(text);
                total += (int)lengths[index];
                index++;
            }
        }

        var bytes = new byte[Math.Max(total, 1)];
        index = 0;

        foreach (var (name, value) in options)
        {
            Encoding.UTF8.GetBytes(name, bytes.AsSpan(offsets[index++]));
            Encoding.UTF8.GetBytes(value, bytes.AsSpan(offsets[index++]));
        }

        var names = new IntPtr[count];
        var values = new IntPtr[count];
        var nameLengths = new ulong[count];
        var valueLengths = new ulong[count];

        using var lease = Environment.Acquire();

        fixed (byte* buffer = bytes)
        fixed (IntPtr* namePointers = names)
        fixed (IntPtr* valuePointers = values)
        fixed (ulong* nameLengthPointer = nameLengths)
        fixed (ulong* valueLengthPointer = valueLengths)
        {
            for (var option = 0; option < count; option++)
            {
                names[option] = (IntPtr)(buffer + offsets[option * 2]);
                nameLengths[option] = lengths[option * 2];
                values[option] = (IntPtr)(buffer + offsets[option * 2 + 1]);
                valueLengths[option] = lengths[option * 2 + 1];
            }

            _duckdb_v2_error_info* error = null;
            _duckdb_v2_instance* instance;
            NativeMethodsV2.duckdb_v2_instance_create_with_options(lease.Pointer,
                                                                  (sbyte**)namePointers, nameLengthPointer,
                                                                  (sbyte**)valuePointers, valueLengthPointer, (ulong)count,
                                                                  &instance, &error).ThrowIfFailed(ref error, "Opening the database failed");

            return new DuckDBV2Instance(instance);
        }
    }

    // V2 sets all startup options in one call, and DuckDB's message for a refused value does not always name the
    // option ("Must have at least 1 thread!"). V1 set them one at a time and could say which one failed, so this finds
    // it again by trying each option on its own. Only on the failure path: an instance without a database is cheap.
    private static DuckDBException NameTheRefusedOption(IReadOnlyDictionary<string, string> options, DuckDBException exception, DuckDBErrorCode code)
    {
        foreach (var (name, value) in options)
        {
            try
            {
                CreateInstance(new Dictionary<string, string> { [name] = value }).Dispose();
            }
            catch (DuckDBException)
            {
                return new DuckDBException($"Error setting '{name}' to '{value}': {exception.Message}", code);
            }
        }

        // Each option is accepted on its own, so it is their combination that DuckDB refuses.
        return exception;
    }

    private static void Attach(DuckDBV2Instance instance, string path)
    {
        using var pathBuffer = DuckDBV2Utf8.FromManaged(path);
        using var lease = instance.Acquire();

        fixed (byte* pathPointer = pathBuffer.Bytes)
        {
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_instance_attach(lease.Pointer, null, 0, (sbyte*)pathPointer, (ulong)pathBuffer.Length,
                                                      null, 1, &error).ThrowIfFailed(ref error, $"Opening '{path}' failed");
        }
    }
}

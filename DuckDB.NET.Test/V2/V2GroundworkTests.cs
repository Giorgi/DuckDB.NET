using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// Stage 0 of docs/V2-MIGRATION.md: the generated bindings, the error codes, the error helper, the UTF-8 strings and
// the SafeHandles.
[Trait("V2", "Ready")]
public unsafe class V2GroundworkTests
{
    [Fact]
    public void PublicErrorCodesMatchTheGeneratedOnes()
    {
        static string Normalize(string name) => name.Replace("_", "").ToUpperInvariant();

        var generated = Enum.GetValues<DUCKDB_V2_ERROR>().Where(code => code != DUCKDB_V2_ERROR.MAX_ENUM)
                            .ToDictionary(code => Normalize(code.ToString()), code => (long)code);
        var published = Enum.GetValues<DuckDBErrorCode>().ToDictionary(code => Normalize(code.ToString()), code => (long)code);

        published.Should().BeEquivalentTo(generated);
    }

    [Fact]
    public void EveryErrorCodeOfAnEngineExceptionMapsToItsErrorType()
    {
        // The codes that no engine exception type produces, and the success code.
        DuckDBErrorCode[] withoutOwnType =
        [
            DuckDBErrorCode.None, DuckDBErrorCode.Api, DuckDBErrorCode.ResourceInUse,
            DuckDBErrorCode.IoFileNotFound, DuckDBErrorCode.IoReadFailure, DuckDBErrorCode.IoEof,
            DuckDBErrorCode.InputParameterInvalid, DuckDBErrorCode.InputMetadataDeferred,
            DuckDBErrorCode.ConfigurationInvalid,
        ];

        var mapped = Enum.GetValues<DuckDBErrorCode>().Except(withoutOwnType)
                         .Select(code => (code, type: code.ToErrorType())).ToList();

        mapped.Should().NotContain(pair => pair.type == DuckDBErrorType.Invalid);
        mapped.Select(pair => pair.type).Should().OnlyHaveUniqueItems("each engine exception type has one V2 code");
    }

    [Theory]
    [InlineData(DuckDBErrorCode.QueryParser, DuckDBErrorType.Parser)]
    [InlineData(DuckDBErrorCode.DatabaseConstraint, DuckDBErrorType.Constraint)]
    [InlineData(DuckDBErrorCode.TypeConversion, DuckDBErrorType.Conversion)]
    [InlineData(DuckDBErrorCode.IoFileNotFound, DuckDBErrorType.Io)]
    [InlineData(DuckDBErrorCode.ResourceInUse, DuckDBErrorType.Invalid)]
    public void ErrorCodeMapsToErrorType(DuckDBErrorCode code, DuckDBErrorType expected)
    {
        code.ToErrorType().Should().Be(expected);
    }

    [Fact]
    public void SuccessDoesNotThrow()
    {
        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_ERROR.NONE.ThrowIfFailed(ref error);
    }

    [Fact]
    public void FailureWithoutErrorInfoThrowsTheFallbackMessage()
    {
        var act = () =>
        {
            _duckdb_v2_error_info* error = null;
            DUCKDB_V2_ERROR.INPUT_INVALID.ThrowIfFailed(ref error, "fallback");
        };

        act.Should().Throw<DuckDBException>().Which.Should()
           .Match<DuckDBException>(e => e.Message == "fallback" && e.NativeErrorCode == DuckDBErrorCode.InputInvalid
                                        && e.ErrorType == DuckDBErrorType.InvalidInput);
    }

    [Fact]
    public void InterruptThrowsOperationCanceled()
    {
        var act = () =>
        {
            _duckdb_v2_error_info* error = null;
            DUCKDB_V2_ERROR.RUNTIME_INTERRUPT.ThrowIfFailed(ref error);
        };

        act.Should().Throw<OperationCanceledException>();
    }

    [Fact]
    public void LibraryVersionMatchesV1()
    {
        _duckdb_v2_error_info* error = null;
        sbyte* version;
        ulong length;
        NativeMethodsV2.duckdb_v2_library_version(&version, &length, &error).ThrowIfFailed(ref error);

        DuckDBV2Utf8.ToManaged(version, length).Should().Be(NativeMethods.Startup.DuckDBLibraryVersion());
    }

    [Fact]
    public void CreatesAndDestroysEnvironmentInstanceAndConnection()
    {
        using var environment = CreateEnvironment();

        var instance = CreateInstance(environment);
        DuckDBV2Connection connection;
        using (var lease = instance.Acquire())
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_connection* pointer;
            NativeMethodsV2.duckdb_v2_connection_create(lease.Pointer, &pointer, &error).ThrowIfFailed(ref error);
            connection = new DuckDBV2Connection(pointer);
        }

        InstanceCount(environment).Should().Be(1);

        // The count is of instance handles. A connection keeps its instance alive after the handle is destroyed, so
        // the instance handle can go first.
        instance.Dispose();
        InstanceCount(environment).Should().Be(0);

        connection.Dispose();
    }

    [Fact]
    public void EnvironmentRefusesToCloseWhileAnInstanceIsAlive()
    {
        var environment = CreateEnvironment();
        var instance = CreateInstance(environment);

        using (var lease = environment.Acquire())
        {
            var pointer = lease.Pointer;
            NativeMethodsV2.duckdb_v2_environment_destroy(&pointer).Should().Be(DUCKDB_V2_ERROR.RESOURCE_IN_USE);
            ((IntPtr)pointer).Should().Be((IntPtr)lease.Pointer, "a refused destroy leaves the handle as it was");
        }

        instance.Dispose();
        environment.Dispose();
    }

    [Fact]
    public void LeaseOfAClosedHandleThrows()
    {
        var environment = CreateEnvironment();
        environment.Dispose();

        var act = () => { using var lease = environment.Acquire(); };

        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void InstanceOptionsArePassedAsUtf8WithLengths()
    {
        using var environment = CreateEnvironment();

        CreateInstance(environment, "threads", "3").Dispose();

        var act = () => CreateInstance(environment, "no_such_option", "1");

        var exception = act.Should().Throw<DuckDBException>().Which;
        exception.Message.Should().Contain("no_such_option");
        exception.NativeErrorCode.Should().NotBeNull().And.NotBe(DuckDBErrorCode.None);
    }

    [Fact]
    public void Utf8RoundTripsNonAsciiText()
    {
        const string text = "中文 한국어 ünïcödé";

        using var buffer = DuckDBV2Utf8.FromManaged(text);

        fixed (byte* bytes = buffer.Bytes)
        {
            DuckDBV2Utf8.ToManaged((sbyte*)bytes, (ulong)buffer.Length).Should().Be(text);
        }

        DuckDBV2Utf8.ToManaged(null, 0).Should().BeEmpty();
    }

    private static DuckDBV2Environment CreateEnvironment()
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_environment* pointer;
        NativeMethodsV2.duckdb_v2_environment_create(&pointer, &error).ThrowIfFailed(ref error);
        return new DuckDBV2Environment(pointer);
    }

    private static DuckDBV2Instance CreateInstance(DuckDBV2Environment environment)
    {
        using var lease = environment.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_instance* pointer;
        NativeMethodsV2.duckdb_v2_instance_create(lease.Pointer, &pointer, &error).ThrowIfFailed(ref error);
        return new DuckDBV2Instance(pointer);
    }

    private static DuckDBV2Instance CreateInstance(DuckDBV2Environment environment, string name, string value)
    {
        using var nameBuffer = DuckDBV2Utf8.FromManaged(name);
        using var valueBuffer = DuckDBV2Utf8.FromManaged(value);
        using var lease = environment.Acquire();

        fixed (byte* namePointer = nameBuffer.Bytes)
        fixed (byte* valuePointer = valueBuffer.Bytes)
        {
            var names = (sbyte*)namePointer;
            var values = (sbyte*)valuePointer;
            var nameLength = (ulong)nameBuffer.Length;
            var valueLength = (ulong)valueBuffer.Length;

            _duckdb_v2_error_info* error = null;
            _duckdb_v2_instance* pointer;
            NativeMethodsV2.duckdb_v2_instance_create_with_options(lease.Pointer, &names, &nameLength, &values, &valueLength, 1,
                                                                  &pointer, &error).ThrowIfFailed(ref error);
            return new DuckDBV2Instance(pointer);
        }
    }

    private static ulong InstanceCount(DuckDBV2Environment environment)
    {
        using var lease = environment.Acquire();
        _duckdb_v2_error_info* error = null;
        ulong count;
        NativeMethodsV2.duckdb_v2_environment_get_instance_count(lease.Pointer, &count, &error).ThrowIfFailed(ref error);
        return count;
    }
}

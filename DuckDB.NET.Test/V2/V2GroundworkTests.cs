using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// Stage 0 of docs/V2-MIGRATION.md: the V2 error codes, the error helper, the UTF-8 strings and the SafeHandles.
public class V2GroundworkTests
{
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
        var error = IntPtr.Zero;
        DuckDBErrorCode.None.ThrowIfFailed(ref error);
    }

    [Fact]
    public void FailureWithoutErrorInfoThrowsTheFallbackMessage()
    {
        var error = IntPtr.Zero;
        var act = () => DuckDBErrorCode.InputInvalid.ThrowIfFailed(ref error, "fallback");

        act.Should().Throw<DuckDBException>().Which.Should()
           .Match<DuckDBException>(e => e.Message == "fallback" && e.NativeErrorCode == DuckDBErrorCode.InputInvalid
                                        && e.ErrorType == DuckDBErrorType.InvalidInput);
    }

    [Fact]
    public void InterruptThrowsOperationCanceled()
    {
        var error = IntPtr.Zero;
        var act = () => DuckDBErrorCode.RuntimeInterrupt.ThrowIfFailed(ref error);

        act.Should().Throw<OperationCanceledException>();
    }

    [Fact]
    public unsafe void LibraryVersionMatchesV1()
    {
        var error = IntPtr.Zero;
        NativeMethodsV2.Startup.DuckDBV2LibraryVersion(out var version, out var length, ref error).ThrowIfFailed(ref error);

        DuckDBV2Utf8.ToManaged(version, length).Should().Be(NativeMethods.Startup.DuckDBLibraryVersion());
    }

    [Fact]
    public void CreatesAndDestroysEnvironmentInstanceAndConnection()
    {
        var error = IntPtr.Zero;
        NativeMethodsV2.Startup.DuckDBV2EnvironmentCreate(out var environment, ref error).ThrowIfFailed(ref error);

        using (environment)
        {
            NativeMethodsV2.Startup.DuckDBV2InstanceCreate(environment, out var instance, ref error).ThrowIfFailed(ref error);
            NativeMethodsV2.Startup.DuckDBV2ConnectionCreate(instance, out var connection, ref error).ThrowIfFailed(ref error);

            InstanceCount(environment).Should().Be(1);

            // A connection keeps its instance alive, so the instance handle can go first.
            instance.Dispose();
            connection.Dispose();

            InstanceCount(environment).Should().Be(0);
        }

        environment.IsClosed.Should().BeTrue();
    }

    [Fact]
    public void EnvironmentRefusesToCloseWhileAnInstanceIsAlive()
    {
        var error = IntPtr.Zero;
        NativeMethodsV2.Startup.DuckDBV2EnvironmentCreate(out var environment, ref error).ThrowIfFailed(ref error);
        NativeMethodsV2.Startup.DuckDBV2InstanceCreate(environment, out var instance, ref error).ThrowIfFailed(ref error);

        var handle = environment.DangerousGetHandle();
        NativeMethodsV2.Startup.DuckDBV2EnvironmentDestroy(ref handle).Should().Be(DuckDBErrorCode.ResourceInUse);
        handle.Should().Be(environment.DangerousGetHandle(), "a refused destroy leaves the handle as it was");

        instance.Dispose();
        environment.Dispose();
    }

    [Fact]
    public unsafe void InstanceOptionsArePassedAsUtf8WithLengths()
    {
        var error = IntPtr.Zero;
        NativeMethodsV2.Startup.DuckDBV2EnvironmentCreate(out var environment, ref error).ThrowIfFailed(ref error);

        using (environment)
        {
            CreateWithOption(environment, "threads", "3").Dispose();

            var act = () => CreateWithOption(environment, "no_such_option", "1");

            var exception = act.Should().Throw<DuckDBException>().Which;
            exception.Message.Should().Contain("no_such_option");
            exception.NativeErrorCode.Should().NotBeNull().And.NotBe(DuckDBErrorCode.None);
        }
    }

    [Fact]
    public unsafe void Utf8RoundTripsNonAsciiText()
    {
        const string text = "中文 한국어 ünïcödé";

        using var buffer = DuckDBV2Utf8.FromManaged(text);

        fixed (byte* bytes = buffer.Bytes)
        {
            DuckDBV2Utf8.ToManaged(bytes, (ulong)buffer.Length).Should().Be(text);
        }

        DuckDBV2Utf8.ToManaged(null, 0).Should().BeEmpty();
    }

    private static ulong InstanceCount(DuckDBV2Environment environment)
    {
        var error = IntPtr.Zero;
        NativeMethodsV2.Startup.DuckDBV2EnvironmentGetInstanceCount(environment, out var count, ref error).ThrowIfFailed(ref error);
        return count;
    }

    private static unsafe DuckDBV2Instance CreateWithOption(DuckDBV2Environment environment, string name, string value)
    {
        using var nameBuffer = DuckDBV2Utf8.FromManaged(name);
        using var valueBuffer = DuckDBV2Utf8.FromManaged(value);

        fixed (byte* namePointer = nameBuffer.Bytes)
        fixed (byte* valuePointer = valueBuffer.Bytes)
        {
            var nameLength = (ulong)nameBuffer.Length;
            var valueLength = (ulong)valueBuffer.Length;

            var error = IntPtr.Zero;
            NativeMethodsV2.Startup.DuckDBV2InstanceCreateWithOptions(environment, &namePointer, &nameLength, &valuePointer, &valueLength,
                                                                     1, out var instance, ref error).ThrowIfFailed(ref error);
            return instance;
        }
    }
}

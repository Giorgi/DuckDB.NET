using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, options);
    }

    public void RegisterScalarFunction<T, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), @params, options, TypeExtensions.GetLogicalType<T>());
    }

    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>());
    }

    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(), TypeExtensions.GetLogicalType<T3>());
    }

    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(),
            TypeExtensions.GetLogicalType<T3>(), TypeExtensions.GetLogicalType<T4>());
    }

    private unsafe void RegisterScalarMethod(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, DuckDBLogicalType returnType,
                                             bool varargs, ScalarFunctionOptions? options, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetName(function, name);

        if (varargs)
        {
            if (parameterTypes.Length != 1)
            {
                throw new InvalidOperationException("Cannot use params with multiple parameters");
            }

            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVarargs(function, parameterTypes[0]);
            parameterTypes[0].Dispose();
        }
        else
        {
            foreach (var type in parameterTypes)
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionAddParameter(function, type);
                type.Dispose();
            }
        }

        // Functions with parameters default to pure; parameterless functions (e.g. random()) default to volatile
        var defaultPure = parameterTypes.Length > 0;
        if (!(options?.IsPureFunction ?? defaultPure))
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVolatile(function);
        }

        if (options?.HandlesNulls == true)
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetSpecialHandling(function);
        }

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetReturnType(function, returnType);
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetFunction(function, &ScalarFunctionCallback);

        var info = new ScalarFunctionInfo(returnType, action);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetExtraInfo(function, info.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.ScalarFunction.DuckDBRegisterScalarFunction(NativeConnection, function);

        NativeMethods.ScalarFunction.DuckDBDestroyScalarFunction(ref function);

        if (!state.IsSuccess())
        {
            throw new InvalidOperationException($"Error registering user defined scalar function: {name}");
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ScalarFunctionCallback(IntPtr info, IntPtr chunk, IntPtr outputVector)
    {
        VectorDataReaderBase[] readers = [];
        VectorDataWriterBase? writer = null;

        try
        {
            var dataChunk = new DuckDBDataChunk(chunk);

            var chunkSize = NativeMethods.DataChunks.DuckDBDataChunkGetSize(dataChunk);
            var handle = GCHandle.FromIntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetExtraInfo(info));

            if (handle.Target is not ScalarFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
            }

            readers = new VectorDataReaderBase[NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(dataChunk)];

            for (var index = 0; index < readers.Length; index++)
            {
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);
                using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
                readers[index] = VectorDataReaderFactory.CreateReader(vector, logicalType);
            }

            writer = VectorDataWriterFactory.CreateWriter(outputVector, functionInfo.ReturnType);

            functionInfo.Action(readers, writer, chunkSize);
        }
        catch (Exception ex)
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetError(info, ex.Message);
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader.Dispose();
            }

            writer?.Dispose();
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void DestroyExtraInfo(IntPtr pointer) => pointer.FreeHandle();
}
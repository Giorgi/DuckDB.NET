using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data.Functions;

// Every scalar function is registered here, by the public methods of DuckDBConnection and of
// DuckDBConnectionScalarFunctionExtensions. DuckDB.ExtensionKit compiles this folder too (see AGENTS.md here), so
// nothing in it depends on the ADO.NET classes: it takes the native connection, and what only DuckDB.NET does with
// the exception of a function goes through UdfErrors.
internal static class ScalarFunctionRegistration
{
    // The action gets the readers and the writer as the classes they are. The public methods of DuckDBConnection take
    // an action over the interfaces, which is an action over the classes as well. The typed functions in
    // TypedScalarFunctions pass an action over the classes: through an interface, GetValue<T> and WriteValue<T> are
    // looked up at run time on every call.
    public static unsafe void Register(DuckDBNativeConnection connection, string name, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action,
                                       DuckDBLogicalType returnType, bool varargs, ScalarFunctionOptions? options, params DuckDBLogicalType[] parameterTypes)
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
        UdfErrors.ConfigureScalarFunction(function);
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetFunction(function, &ScalarFunctionCallback);

        var info = new ScalarFunctionInfo(returnType, action);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetExtraInfo(function, info.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.ScalarFunction.DuckDBRegisterScalarFunction(connection, function);

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
            UdfErrors.ScalarFunctionFailed(info, ex);

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

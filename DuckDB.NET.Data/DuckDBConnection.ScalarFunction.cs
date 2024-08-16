using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Data.Reader;
using DuckDB.NET.Data.Writer;
using DuckDB.NET.Native;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
#if NET6_0_OR_GREATER
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, bool isPureFunction = false)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: false, !isPureFunction);
    }

    public void RegisterScalarFunction<T, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, bool isPureFunction = true, bool @params = false)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), @params, !isPureFunction, DuckDBTypeMap.GetLogicalType<T>());
    }

    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              DuckDBTypeMap.GetLogicalType<T1>(), 
                              DuckDBTypeMap.GetLogicalType<T2>());
    }

    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              DuckDBTypeMap.GetLogicalType<T1>(),
                              DuckDBTypeMap.GetLogicalType<T2>(),
                              DuckDBTypeMap.GetLogicalType<T3>());
    }

    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, DuckDBTypeMap.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              DuckDBTypeMap.GetLogicalType<T1>(),
                              DuckDBTypeMap.GetLogicalType<T2>(),
                              DuckDBTypeMap.GetLogicalType<T3>(),
                              DuckDBTypeMap.GetLogicalType<T4>());
    }

    private unsafe void RegisterScalarMethod(string name, Action<IDuckDBDataReader[], IDuckDBDataWriter, ulong> action, DuckDBLogicalType returnType,
                                             bool varargs, bool @volatile, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetName(function, name.ToUnmanagedString());

        if (varargs)
        {
            if (parameterTypes.Length != 1)
            {
                throw new InvalidOperationException("Cannot use params with multiple parameters");
            }

            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVarargs(function, parameterTypes[0]);
        }
        else
        {
            foreach (var type in parameterTypes)
            {
                NativeMethods.ScalarFunction.DuckDBScalarFunctionAddParameter(function, type);
                type.Dispose();
            }
        }

        if (@volatile)
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetVolatile(function);
        }

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetReturnType(function, returnType);
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetFunction(function, &ScalarFunctionCallback);

        var info = new ScalarFunctionInfo(returnType, action);

        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetExtraInfo(function, info.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.ScalarFunction.DuckDBRegisterScalarFunction(NativeConnection, function);

        NativeMethods.ScalarFunction.DuckDBDestroyScalarFunction(out function);

        if (!state.IsSuccess())
        {
            throw new InvalidOperationException("Error registering user defined scalar function");
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ScalarFunctionCallback(IntPtr info, IntPtr chunk, IntPtr outputVector)
    {
        var dataChunk = new DuckDBDataChunk(chunk);

        var chunkSize = NativeMethods.DataChunks.DuckDBDataChunkGetSize(dataChunk);
        var handle = GCHandle.FromIntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetExtraInfo(info));

        if (handle.Target is not ScalarFunctionInfo functionInfo)
        {
            throw new InvalidOperationException("User defined scalar function execution failed. Function extra info is null");
        }

        var readers = new VectorDataReaderBase[NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(dataChunk)];

        for (var index = 0; index < readers.Length; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);
            readers[index] = VectorDataReaderFactory.CreateReader(vector, NativeMethods.Vectors.DuckDBVectorGetColumnType(vector));
        }

        var writer = VectorDataWriterFactory.CreateWriter(outputVector, functionInfo.ReturnType);

        functionInfo.Action(readers, writer, chunkSize);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void DestroyExtraInfo(IntPtr pointer) => pointer.FreeHandle();
#endif
}
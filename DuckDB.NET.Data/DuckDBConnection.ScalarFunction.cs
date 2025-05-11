using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
#if NET8_0_OR_GREATER
    [Experimental("DuckDBNET001")]
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, bool isPureFunction = false)
    {
        RegisterScalarMethod(name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction);
    }

    [Obsolete("Prefer using RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, bool isPureFunction = false)")]
    [Experimental("DuckDBNET001")]
#pragma warning disable DuckDBNET001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
    public void RegisterScalarFunction<TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = false)
    {

        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction);
    }
#pragma warning restore DuckDBNET001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.

    [Experimental("DuckDBNET001")]
    public void RegisterScalarFunction<T, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true, bool @params = false)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), @params, !isPureFunction, TypeExtensions.GetLogicalType<T>());
    }

    [Experimental("DuckDBNET001")]
    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(), 
                              TypeExtensions.GetLogicalType<T2>());
    }

    [Experimental("DuckDBNET001")]
    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(),
                              TypeExtensions.GetLogicalType<T2>(),
                              TypeExtensions.GetLogicalType<T3>());
    }

    [Experimental("DuckDBNET001")]
    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, bool isPureFunction = true)
    {
        RegisterScalarMethod(name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, !isPureFunction,
                              TypeExtensions.GetLogicalType<T1>(),
                              TypeExtensions.GetLogicalType<T2>(),
                              TypeExtensions.GetLogicalType<T3>(),
                              TypeExtensions.GetLogicalType<T4>());
    }

    [Experimental("DuckDBNET001")]
    private unsafe void RegisterScalarMethod(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, DuckDBLogicalType returnType,
                                             bool varargs, bool @volatile, params DuckDBLogicalType[] parameterTypes)
    {
        var function = NativeMethods.ScalarFunction.DuckDBCreateScalarFunction();
        using (var handle = name.ToUnmanagedString())
        {
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetName(function, handle);
        }

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
            throw new InvalidOperationException($"Error registering user defined scalar function: {name}");
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
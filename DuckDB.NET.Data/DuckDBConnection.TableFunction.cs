using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

public record ColumnInfo(string Name, Type Type);

public record TableFunction(IReadOnlyList<ColumnInfo> Columns, IEnumerable Data);

partial class DuckDBConnection
{
#if NET8_0_OR_GREATER
    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction(string name, Func<TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, (_) => resultCallback(), mapperCallback);
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3, T4>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3, T4, T5>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7));
    }

    [Experimental("DuckDBNET001")]
    public void RegisterTableFunction<T1, T2, T3, T4, T5, T6, T7, T8>(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback)
    {
        RegisterTableFunctionInternal(name, resultCallback, mapperCallback, typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6), typeof(T7), typeof(T8));
    }

    [Experimental("DuckDBNET001")]
    private unsafe void RegisterTableFunctionInternal(string name, Func<IReadOnlyList<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[], ulong> mapperCallback, params Type[] parameterTypes)
    {
        var function = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        using (var handle = name.ToUnmanagedString())
        {
            NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, handle);
        }

        foreach (var type in parameterTypes)
        {
            using var logicalType = type.GetLogicalType();
            NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
        }

        var tableFunctionInfo = new TableFunctionInfo(resultCallback, mapperCallback);

        NativeMethods.TableFunction.DuckDBTableFunctionSetBind(function, &Bind);
        NativeMethods.TableFunction.DuckDBTableFunctionSetInit(function, &Init);
        NativeMethods.TableFunction.DuckDBTableFunctionSetFunction(function, &TableFunction);
        NativeMethods.TableFunction.DuckDBTableFunctionSetExtraInfo(function, tableFunctionInfo.ToHandle(), &DestroyExtraInfo);

        var state = NativeMethods.TableFunction.DuckDBRegisterTableFunction(NativeConnection, function);

        if (!state.IsSuccess())
        {
            throw new InvalidOperationException($"Error registering user defined table function: {name}");
        }

        NativeMethods.TableFunction.DuckDBDestroyTableFunction(ref function);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void Bind(IntPtr info)
    {
        IDuckDBValueReader[] parameters = [];
        try
        {
            var handle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info));

            if (handle.Target is not TableFunctionInfo functionInfo)
            {
                throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
            }

            parameters = new IDuckDBValueReader[NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

            for (var i = 0; i < parameters.Length; i++)
            {
                var value = NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
                parameters[i] = value;
            }

            var tableFunctionData = functionInfo.Bind(parameters);

            foreach (var columnInfo in tableFunctionData.Columns)
            {
                using var logicalType = columnInfo.Type.GetLogicalType();
                NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnInfo.Name.ToUnmanagedString(), logicalType);
            }

            var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data.GetEnumerator());

            NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
        }
        catch (Exception ex)
        {
            using var errorMessage = ex.Message.ToUnmanagedString();
            NativeMethods.TableFunction.DuckDBBindSetError(info, errorMessage);
        }
        finally
        {
            foreach (var parameter in parameters)
            {
                (parameter as IDisposable)?.Dispose();
            }
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static void Init(IntPtr info) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static void TableFunction(IntPtr info, IntPtr chunk)
    {
        try
        {
            var bindData = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetBindData(info));
            var extraInfo = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBFunctionGetExtraInfo(info));

            if (bindData.Target is not TableFunctionBindData tableFunctionBindData)
            {
                throw new InvalidOperationException("User defined table function failed. Function bind data is null");
            }

            if (extraInfo.Target is not TableFunctionInfo tableFunctionInfo)
            {
                throw new InvalidOperationException("User defined table function failed. Function extra info is null");
            }

            var dataChunk = new DuckDBDataChunk(chunk);

            var writers = new VectorDataWriterBase[tableFunctionBindData.Columns.Count];
            for (var columnIndex = 0; columnIndex < tableFunctionBindData.Columns.Count; columnIndex++)
            {
                var column = tableFunctionBindData.Columns[columnIndex];
                var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, columnIndex);

                using var logicalType = column.Type.GetLogicalType();
                writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, logicalType);
            }

            ulong size = 0;

            for (; size < DuckDBGlobalData.VectorSize; size++)
            {
                if (tableFunctionBindData.DataEnumerator.MoveNext())
                {
                    tableFunctionInfo.Mapper(tableFunctionBindData.DataEnumerator.Current, writers, size);
                }
                else
                {
                    break;
                }
            }

            NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
        }
        catch (Exception ex)
        {
            using var errorMessage = ex.Message.ToUnmanagedString();
            NativeMethods.TableFunction.DuckDBFunctionSetError(info, errorMessage);
        }
    }
#endif
}
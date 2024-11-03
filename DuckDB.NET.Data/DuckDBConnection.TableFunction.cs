using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Data.Writer;

namespace DuckDB.NET.Data;

public record ColumnInfo(string Name, Type Type)
{
}

public record TableFunction(IReadOnlyList<ColumnInfo> Columns, IEnumerable Data)
{
}

partial class DuckDBConnection
{
#if NET8_0_OR_GREATER
    [Experimental("DuckDBNET001")]
    public unsafe void RegisterTableFunction<T>(string name, Func<IEnumerable<IDuckDBValueReader>, TableFunction> resultCallback, Action<object?, IDuckDBDataWriter[]> mapperCallback)
    {
        var function = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, name.ToUnmanagedString());

        using (var logicalType = DuckDBTypeMap.GetLogicalType<T>())
        {
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

        NativeMethods.TableFunction.DuckDBDestroyTableFunction(out function);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void Bind(IntPtr info)
    {
        var handle = GCHandle.FromIntPtr(NativeMethods.TableFunction.DuckDBBindGetExtraInfo(info));

        if (handle.Target is not TableFunctionInfo functionInfo)
        {
            throw new InvalidOperationException("User defined table function bind failed. Bind extra info is null");
        }

        var parameters = new IDuckDBValueReader[NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

        for (var i = 0; i < parameters.Length; i++)
        {
            var value = NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
            parameters[i] = value;
        }

        var tableFunctionData = functionInfo.Bind(parameters);

        foreach (var parameter in parameters)
        {
            (parameter as IDisposable)?.Dispose();
        }

        foreach (var columnInfo in tableFunctionData.Columns)
        {
            using var logicalType = DuckDBTypeMap.GetLogicalType(columnInfo.Type);
            NativeMethods.TableFunction.DuckDBBindAddResultColumn(info, columnInfo.Name.ToUnmanagedString(), logicalType);
        }

        var bindData = new TableFunctionBindData(tableFunctionData.Columns, tableFunctionData.Data.GetEnumerator());

        NativeMethods.TableFunction.DuckDBBindSetBindData(info, bindData.ToHandle(), &DestroyExtraInfo);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void Init(IntPtr info) { }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void TableFunction(IntPtr info, IntPtr chunk)
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

            using var logicalType = DuckDBTypeMap.GetLogicalType(column.Type);
            writers[columnIndex] = VectorDataWriterFactory.CreateWriter(vector, logicalType);
        }

        ulong size = 0;

        for (; size < DuckDBGlobalData.VectorSize; size++)
        {
            if (tableFunctionBindData.DataEnumerator.MoveNext())
            {
                tableFunctionInfo.Mapper(tableFunctionBindData.DataEnumerator.Current, writers);
            }
            else
            {
                break;
            }
        }

        NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, size);
    }
#endif
}
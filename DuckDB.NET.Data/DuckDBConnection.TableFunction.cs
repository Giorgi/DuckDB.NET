using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
    public unsafe void RegisterTableFunction<T, TResult>(string name)
    {
        var function = NativeMethods.TableFunction.DuckDBCreateTableFunction();
        NativeMethods.TableFunction.DuckDBTableFunctionSetName(function, name.ToUnmanagedString());

        using (var logicalType = DuckDBTypeMap.GetLogicalType<T>())
        {
            NativeMethods.TableFunction.DuckDBTableFunctionAddParameter(function, logicalType);
        }

        NativeMethods.TableFunction.DuckDBTableFunctionSetBind(function, &Bind);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    public static unsafe void Bind(IntPtr info)
    {
        var parameters = new object[NativeMethods.TableFunction.DuckDBBindGetParameterCount(info)];

        for (var i = 0; i < parameters.Length; i++)
        {
            using var value = NativeMethods.TableFunction.DuckDBBindGetParameter(info, (ulong)i);
            parameters[i] = NativeMethods.Value.DuckDBGetInt32(value);
        }

        NativeMethods.TableFunction.DuckDBBindSetBindData(info, parameters.ToHandle(), &DestroyExtraInfo);
    }
}
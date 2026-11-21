using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;

namespace DuckDB.NET.Data;

// The registration itself is in Functions/ScalarFunctionRegistration.cs, which DuckDB.ExtensionKit shares.
partial class DuckDBConnection
{
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(NativeConnection, name, (_, w, index) => action(w, index), TypeExtensions.GetLogicalType<TResult>(), varargs: false, options);
    }

    public void RegisterScalarFunction<T, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        ScalarFunctionRegistration.Register(NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), @params, options, TypeExtensions.GetLogicalType<T>());
    }

    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>());
    }

    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(), TypeExtensions.GetLogicalType<T3>());
    }

    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(NativeConnection, name, action, TypeExtensions.GetLogicalType<TResult>(), varargs: false, options,
            TypeExtensions.GetLogicalType<T1>(), TypeExtensions.GetLogicalType<T2>(),
            TypeExtensions.GetLogicalType<T3>(), TypeExtensions.GetLogicalType<T4>());
    }
}

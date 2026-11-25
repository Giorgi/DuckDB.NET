using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Functions;

namespace DuckDB.NET.Data;

// The registration itself is in Functions/ScalarFunctionRegistration.cs, which DuckDB.ExtensionKit shares.
partial class DuckDBConnection
{
    public void RegisterScalarFunction<TResult>(string name, Action<IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(V2Connection, name, (_, w, index) => action(w, index), typeof(TResult), varargs: false, options);
    }

    public void RegisterScalarFunction<T, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null, bool @params = false)
    {
        ScalarFunctionRegistration.Register(V2Connection, name, action, typeof(TResult), @params, options, typeof(T));
    }

    public void RegisterScalarFunction<T1, T2, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(V2Connection, name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2));
    }

    public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(V2Connection, name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2), typeof(T3));
    }

    public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> action, ScalarFunctionOptions? options = null)
    {
        ScalarFunctionRegistration.Register(V2Connection, name, action, typeof(TResult), varargs: false, options,
            typeof(T1), typeof(T2),
            typeof(T3), typeof(T4));
    }
}

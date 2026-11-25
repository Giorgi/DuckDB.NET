using DuckDB.NET.Data.Functions;

namespace DuckDB.NET.Data;

// The typed scalar functions. What they do is in Functions/TypedScalarFunctions.cs, which DuckDB.ExtensionKit shares.
public static class DuckDBConnectionScalarFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
            => TypedScalarFunctions.Register(connection.V2Connection, name, func);

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func)
            => TypedScalarFunctions.Register(connection.V2Connection, name, func);

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func)
            => TypedScalarFunctions.RegisterVarargs(connection.V2Connection, name, func);

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func)
            => TypedScalarFunctions.Register(connection.V2Connection, name, func);

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func)
            => TypedScalarFunctions.Register(connection.V2Connection, name, func);

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func)
            => TypedScalarFunctions.Register(connection.V2Connection, name, func);
    }
}

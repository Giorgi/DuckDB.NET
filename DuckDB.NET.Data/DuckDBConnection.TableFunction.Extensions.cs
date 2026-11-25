using DuckDB.NET.Data.Functions;
using System.Linq.Expressions;

namespace DuckDB.NET.Data;

// The typed table functions. What they do is in Functions/TypedTableFunctions.cs, which DuckDB.ExtensionKit shares.
public static class DuckDBConnectionTableFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterTableFunction<TData, TProjection>(
            string name,
            Func<IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.V2Connection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, TData, TProjection>(
            string name,
            Func<T1, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.V2Connection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, TData, TProjection>(
            string name,
            Func<T1, T2, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.V2Connection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, T3, TData, TProjection>(
            string name,
            Func<T1, T2, T3, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.V2Connection, name, dataFunc, projection);

        public void RegisterTableFunction<T1, T2, T3, T4, TData, TProjection>(
            string name,
            Func<T1, T2, T3, T4, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
            => TypedTableFunctions.Register(connection.V2Connection, name, dataFunc, projection);
    }
}

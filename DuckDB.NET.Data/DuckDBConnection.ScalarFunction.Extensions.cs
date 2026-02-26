using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionScalarFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func, bool isPureFunction = false)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            }, isPureFunction);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T, TResult>(name, WrapVarargsScalarFunction(func), isPureFunction, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index), readers[2].GetValue<T3>(index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T1>(index), readers[1].GetValue<T2>(index),
                    readers[2].GetValue<T3>(index), readers[3].GetValue<T4>(index))), isPureFunction);
        }
    }

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapScalarFunction<TResult>(Func<IReadOnlyList<IDuckDBDataReader>, ulong, TResult> perRowFunc)
    {
        return (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var result = perRowFunc(readers, index);

                writer.WriteValue(result, index);
            }
        };
    }

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Count];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Count; r++)
                {
                    args[r] = readers[r].GetValue<T>(index);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }
}

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
                    var result = func();

                    if (result is null)
                    {
                        writer.WriteNull(index);
                    }
                    else
                    {
                        writer.WriteValue(result, index);
                    }
                }
            }, isPureFunction);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(readers[0].GetValue<T>(index))), isPureFunction);
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

                if (result is null)
                {
                    writer.WriteNull(index);
                }
                else
                {
                    writer.WriteValue(result, index);
                }
            }
        };
    }
}

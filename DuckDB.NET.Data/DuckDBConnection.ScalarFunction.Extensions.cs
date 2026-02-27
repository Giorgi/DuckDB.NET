using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;

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
                func(ReadValue<T>(readers[0], index))), isPureFunction);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T, TResult>(name, WrapVarargsScalarFunction(func), isPureFunction, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index), ReadValue<T2>(readers[1], index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index), ReadValue<T2>(readers[1], index), ReadValue<T3>(readers[2], index))), isPureFunction);
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func, bool isPureFunction = true)
        {
            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index), ReadValue<T2>(readers[1], index),
                    ReadValue<T3>(readers[2], index), ReadValue<T4>(readers[3], index))), isPureFunction);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(IDuckDBDataReader reader, ulong index)
        => typeof(T) == typeof(object) ? (T)reader.GetValue(index) : reader.GetValue<T>(index);

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
                    args[r] = ReadValue<T>(readers[r], index);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }
}

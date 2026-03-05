using System.Linq;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionScalarFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func, ScalarFunctionOptions? options = null)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            }, options);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T>(readers[0], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T, TResult>(name,
                WrapVarargsScalarFunction(func, handlesNulls), options, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2), typeof(T3));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls),
                     ReadValue<T3>(readers[2], index, handlesNulls))), options);
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func, ScalarFunctionOptions? options = null)
        {
            ValidateHandlesNulls(options, typeof(T1), typeof(T2), typeof(T3), typeof(T4));
            var handlesNulls = options?.HandlesNulls ?? false;

            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, handlesNulls), ReadValue<T2>(readers[1], index, handlesNulls),
                     ReadValue<T3>(readers[2], index, handlesNulls), ReadValue<T4>(readers[3], index, handlesNulls))), options);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(IDuckDBDataReader reader, ulong index, bool handlesNulls)
    {
        if (typeof(T) == typeof(object))
        {
            if (handlesNulls && !reader.IsValid(index)) return default!;
            return (T)reader.GetValue(index);
        }

        if (handlesNulls && !reader.IsValid(index))
        {
            if (default(T) is null) return default!;
            ThrowNullReceivedByNonNullableParam<T>();
        }

        return reader.GetValue<T>(index);
    }

    private static void ThrowNullReceivedByNonNullableParam<T>()
    {
        throw new InvalidOperationException(
            $"Scalar function parameter of type '{typeof(T).Name}' received NULL. " +
            $"Use '{typeof(T).Name}?' to handle NULL values.");
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

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func, bool handlesNulls)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Count];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Count; r++)
                {
                    args[r] = ReadValue<T>(readers[r], index, handlesNulls);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }

    //If HandlesNulls is true, at least one parameter type must be nullable to allow null values to be passed in.
    private static void ValidateHandlesNulls(ScalarFunctionOptions? options, params Type[] parameterTypes)
    {
        if (options?.HandlesNulls == true && !parameterTypes.Any(t => t.AllowsNullValue(out _, out _)))
            throw new ArgumentException("HandlesNulls requires at least one nullable parameter type (use int? instead of int).");
    }
}

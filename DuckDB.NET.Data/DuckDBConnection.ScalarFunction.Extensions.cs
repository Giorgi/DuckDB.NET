using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionScalarFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
        {
            connection.RegisterScalarFunction<TResult>(name, (writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            });
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T>(readers[0], index, nullability[0], anyNullable))), new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func)
        {
            var elementNullable = InferArrayElementNullability(func);

            connection.RegisterScalarFunction<T, TResult>(name,
                WrapVarargsScalarFunction(func, elementNullable), new() { HandlesNulls = elementNullable }, @params: true);
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, T3, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable),
                     ReadValue<T3>(readers[2], index, nullability[2], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarFunction<T1, T2, T3, T4, TResult>(name, WrapScalarFunction<TResult>((readers, index) =>
                func(ReadValue<T1>(readers[0], index, nullability[0], anyNullable), ReadValue<T2>(readers[1], index, nullability[1], anyNullable),
                     ReadValue<T3>(readers[2], index, nullability[2], anyNullable), ReadValue<T4>(readers[3], index, nullability[3], anyNullable))),
                new() { HandlesNulls = anyNullable });
        }
    }

    // checksNulls: true when any parameter in the function is nullable (special handling active).
    // Needed because set_special_handling is function-level — DuckDB sends NULLs for ALL params,
    // so non-nullable params must also check and throw a descriptive error.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(IDuckDBDataReader reader, ulong index, bool isNullable, bool checksNulls)
    {
        if (checksNulls && !reader.IsValid(index))
        {
            if (isNullable) return default!;
            ThrowNullReceivedByNonNullableParam<T>();
        }

        return typeof(T) == typeof(object) ? (T)reader.GetValue(index) : reader.GetValue<T>(index);
    }

    [DoesNotReturn]
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

    private static Action<IReadOnlyList<IDuckDBDataReader>, IDuckDBDataWriter, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func, bool elementNullable)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Count];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Count; r++)
                {
                    args[r] = ReadValue<T>(readers[r], index, elementNullable, elementNullable);
                }

                writer.WriteValue(func(args), index);
            }
        };
    }

    private static (bool[] perParam, bool anyNullable) InferParameterNullability(Delegate func)
    {
        var context = new NullabilityInfoContext();
        var parameters = func.Method.GetParameters();
        var result = parameters.Select(info => IsNullableParameter(context, info)).ToArray();

        return (result, result.Any(static x => x));
    }

    private static bool InferArrayElementNullability(Delegate func)
    {
        var parameter = func.Method.GetParameters()[0];
        var elementType = parameter.ParameterType.GetElementType();

        // Nullable<T> value types: detectable without attributes
        if (elementType != null && Nullable.GetUnderlyingType(elementType) != null)
            return true;

        // Reference types: check nullable annotation
        var context = new NullabilityInfoContext();
        var info = context.Create(parameter);
        return info.ElementType?.ReadState == NullabilityState.Nullable;
    }

    private static bool IsNullableParameter(NullabilityInfoContext context, ParameterInfo parameter)
    {
        // Nullable<T> value types are always nullable
        if (Nullable.GetUnderlyingType(parameter.ParameterType) != null)
            return true;

        // Reference types: check nullable annotation
        var info = context.Create(parameter);
        return info.ReadState == NullabilityState.Nullable;
    }

}

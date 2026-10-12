using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionScalarFunctionExtensions
{
    // The functions registered here read and write through VectorDataReaderBase and VectorDataWriterBase, not through
    // the public interfaces. GetValue<T> and WriteValue<T> are generic methods: called through an interface, such a
    // method is looked up at run time on every call, which is once or more for every row. On the classes they are
    // called directly.
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
        {
            connection.RegisterScalarMethod(name, (_, writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(), index);
                }
            }, LogicalType<TResult>(), varargs: false, options: null);
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarMethod(name, (readers, writer, rowCount) =>
            {
                var reader = readers[0];
                var nullable = nullability[0];

                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(ReadValue<T>(reader, index, nullable, anyNullable)), index);
                }
            }, LogicalType<TResult>(), varargs: false, new() { HandlesNulls = anyNullable }, LogicalType<T>());
        }

        public void RegisterScalarFunction<T, TResult>(string name, Func<T[], TResult> func)
        {
            var elementNullable = InferArrayElementNullability(func);

            connection.RegisterScalarMethod(name, WrapVarargsScalarFunction(func, elementNullable), LogicalType<TResult>(),
                varargs: true, new() { HandlesNulls = elementNullable }, LogicalType<T>());
        }

        public void RegisterScalarFunction<T1, T2, TResult>(string name, Func<T1, T2, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarMethod(name, (readers, writer, rowCount) =>
            {
                var reader1 = readers[0];
                var reader2 = readers[1];
                var (nullable1, nullable2) = (nullability[0], nullability[1]);

                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable)), index);
                }
            }, LogicalType<TResult>(), varargs: false, new() { HandlesNulls = anyNullable },
            LogicalType<T1>(), LogicalType<T2>());
        }

        public void RegisterScalarFunction<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarMethod(name, (readers, writer, rowCount) =>
            {
                var reader1 = readers[0];
                var reader2 = readers[1];
                var reader3 = readers[2];
                var (nullable1, nullable2, nullable3) = (nullability[0], nullability[1], nullability[2]);

                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable),
                                           ReadValue<T3>(reader3, index, nullable3, anyNullable)), index);
                }
            }, LogicalType<TResult>(), varargs: false, new() { HandlesNulls = anyNullable },
            LogicalType<T1>(), LogicalType<T2>(), LogicalType<T3>());
        }

        public void RegisterScalarFunction<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> func)
        {
            var (nullability, anyNullable) = InferParameterNullability(func);

            connection.RegisterScalarMethod(name, (readers, writer, rowCount) =>
            {
                var reader1 = readers[0];
                var reader2 = readers[1];
                var reader3 = readers[2];
                var reader4 = readers[3];
                var (nullable1, nullable2, nullable3, nullable4) = (nullability[0], nullability[1], nullability[2], nullability[3]);

                for (ulong index = 0; index < rowCount; index++)
                {
                    writer.WriteValue(func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable),
                                           ReadValue<T3>(reader3, index, nullable3, anyNullable), ReadValue<T4>(reader4, index, nullable4, anyNullable)), index);
                }
            }, LogicalType<TResult>(), varargs: false, new() { HandlesNulls = anyNullable },
            LogicalType<T1>(), LogicalType<T2>(), LogicalType<T3>(), LogicalType<T4>());
        }
    }

    // System.Reflection has a TypeExtensions class too, so the one of this library is named in full, once.
    private static DuckDBLogicalType LogicalType<T>() => Extensions.TypeExtensions.GetLogicalType<T>();

    // checksNulls: true when any parameter in the function is nullable (special handling active).
    // Needed because set_special_handling is function-level — DuckDB sends NULLs for ALL params,
    // so non-nullable params must also check and throw a descriptive error.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(VectorDataReaderBase reader, ulong index, bool isNullable, bool checksNulls)
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

    private static Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> WrapVarargsScalarFunction<T, TResult>(Func<T[], TResult> func, bool elementNullable)
    {
        return (readers, writer, rowCount) =>
        {
            var args = new T[readers.Length];

            for (ulong index = 0; index < rowCount; index++)
            {
                for (int r = 0; r < readers.Length; r++)
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

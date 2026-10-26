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
    //
    // NULL arguments. With no nullable parameter, a function is registered without special handling. DuckDB then
    // answers a constant NULL argument itself, but for a column it calls the function with the whole chunk, NULL rows
    // included. So the loops test every argument of every row and write NULL without calling the function when one is
    // NULL: a column behaves as a constant does.
    extension(DuckDBConnection connection)
    {
        public void RegisterScalarFunction<TResult>(string name, Func<TResult> func)
        {
            connection.RegisterScalarMethod(name, (_, writer, rowCount) =>
            {
                for (ulong index = 0; index < rowCount; index++)
                {
                    var result = func();
                    if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
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
                    if (!anyNullable && !reader.IsValid(index))
                    {
                        writer.WriteNull(index);
                        continue;
                    }

                    var result = func(ReadValue<T>(reader, index, nullable, anyNullable));
                    if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
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
                    if (!anyNullable && !(reader1.IsValid(index) && reader2.IsValid(index)))
                    {
                        writer.WriteNull(index);
                        continue;
                    }

                    var result = func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable));
                    if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
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
                    if (!anyNullable && !(reader1.IsValid(index) && reader2.IsValid(index) && reader3.IsValid(index)))
                    {
                        writer.WriteNull(index);
                        continue;
                    }

                    var result = func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable),
                                    ReadValue<T3>(reader3, index, nullable3, anyNullable));
                    if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
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
                    if (!anyNullable && !(reader1.IsValid(index) && reader2.IsValid(index) && reader3.IsValid(index) && reader4.IsValid(index)))
                    {
                        writer.WriteNull(index);
                        continue;
                    }

                    var result = func(ReadValue<T1>(reader1, index, nullable1, anyNullable), ReadValue<T2>(reader2, index, nullable2, anyNullable),
                                    ReadValue<T3>(reader3, index, nullable3, anyNullable), ReadValue<T4>(reader4, index, nullable4, anyNullable));
                    if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
                }
            }, LogicalType<TResult>(), varargs: false, new() { HandlesNulls = anyNullable },
            LogicalType<T1>(), LogicalType<T2>(), LogicalType<T3>(), LogicalType<T4>());
        }
    }

    // System.Reflection has a TypeExtensions class too, so the one of this library is named in full, once.
    private static DuckDBLogicalType LogicalType<T>() => Extensions.TypeExtensions.GetLogicalType<T>();

    // The result of a function. A nullable result is taken apart here and its value written as the type under it, or
    // a NULL. Passed on whole as WriteValue<int?>, it cost about 4 ns more per row than an int (.NET 10, x64,
    // FunctionBenchmark), for two reasons. About 3 ns: when the JIT inlines the function, it builds the int? on the
    // stack with two narrow stores and reads it back as one 8-byte value for the call, and the processor has to wait
    // for the two stores to finish before that load can be served. About 1 ns: WriteValue<int?> hands it on by
    // reference to WriteNullable, which is not inlined. Here the two fields are read one at a time and the value goes
    // to WriteValue of the type under it. The cast through object is folded by the JIT and boxes nothing; Unsafe.As
    // takes the address of the result, and the JIT then copies it with the same 8-byte load.
    //
    // The lines are the value types a function can return: those of ClrToDuckDBTypeMap in TypeExtensions, and the
    // same as in VectorDataWriterBase.WriteNullable. Keep the three in step; a type missing here is still written,
    // through WriteValue as before.
    //
    // The loops call it for a nullable result only, and send any other result straight to WriteValue: the .NET 8 JIT
    // does not inline it into the loops, and a call that only passes the value on cost about 17% on an int function.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteResult<TResult>(VectorDataWriterBase writer, TResult result, ulong index)
    {
        if (typeof(TResult).IsValueType && default(TResult) is null)
        {
            if (typeof(TResult) == typeof(bool?)) { WriteNullableResult(writer, (bool?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(sbyte?)) { WriteNullableResult(writer, (sbyte?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(short?)) { WriteNullableResult(writer, (short?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(int?)) { WriteNullableResult(writer, (int?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(long?)) { WriteNullableResult(writer, (long?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(byte?)) { WriteNullableResult(writer, (byte?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(ushort?)) { WriteNullableResult(writer, (ushort?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(uint?)) { WriteNullableResult(writer, (uint?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(ulong?)) { WriteNullableResult(writer, (ulong?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(float?)) { WriteNullableResult(writer, (float?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(double?)) { WriteNullableResult(writer, (double?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(decimal?)) { WriteNullableResult(writer, (decimal?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(BigInteger?)) { WriteNullableResult(writer, (BigInteger?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(Guid?)) { WriteNullableResult(writer, (Guid?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(DateTime?)) { WriteNullableResult(writer, (DateTime?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(TimeSpan?)) { WriteNullableResult(writer, (TimeSpan?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(DuckDBDateOnly?)) { WriteNullableResult(writer, (DuckDBDateOnly?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(DuckDBTimeOnly?)) { WriteNullableResult(writer, (DuckDBTimeOnly?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(DateOnly?)) { WriteNullableResult(writer, (DateOnly?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(TimeOnly?)) { WriteNullableResult(writer, (TimeOnly?)(object?)result, index); return; }
            if (typeof(TResult) == typeof(DateTimeOffset?)) { WriteNullableResult(writer, (DateTimeOffset?)(object?)result, index); return; }
        }

        writer.WriteValue(result, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteNullableResult<TValue>(VectorDataWriterBase writer, TValue? value, ulong index) where TValue : struct
    {
        if (value.HasValue)
        {
            writer.WriteValue(value.GetValueOrDefault(), index);
        }
        else
        {
            writer.WriteNull(index);
        }
    }

    // checksNulls: true when any parameter in the function is nullable (special handling active).
    // Needed because set_special_handling is function-level — DuckDB sends NULLs for ALL params,
    // so non-nullable params must also check and throw a descriptive error.
    // When it is false, the loop has already tested every argument of the row, so the value is read without a second
    // test. T is then not a Nullable<>, since such a parameter would have made checksNulls true.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T ReadValue<T>(VectorDataReaderBase reader, ulong index, bool isNullable, bool checksNulls)
    {
        if (checksNulls)
        {
            if (!reader.IsValid(index))
            {
                if (isNullable) return default!;
                ThrowNullReceivedByNonNullableParam<T>();
            }

            return typeof(T) == typeof(object) ? (T)reader.GetValue(index) : reader.GetValue<T>(index);
        }

        return typeof(T) == typeof(object) ? (T)reader.GetValue(index, reader.ClrType) : reader.ReadValidValue<T>(index);
    }

    private static bool AllValid(VectorDataReaderBase[] readers, ulong index)
    {
        foreach (var reader in readers)
        {
            if (!reader.IsValid(index)) return false;
        }

        return true;
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
                if (!elementNullable && !AllValid(readers, index))
                {
                    writer.WriteNull(index);
                    continue;
                }

                for (int r = 0; r < readers.Length; r++)
                {
                    args[r] = ReadValue<T>(readers[r], index, elementNullable, elementNullable);
                }

                var result = func(args);
                if (typeof(TResult).IsValueType && default(TResult) is null) WriteResult(writer, result, index); else writer.WriteValue(result, index);
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

        // Reference types: check nullable annotation. `string?` makes both states nullable; [AllowNull], which says
        // callers may pass null, makes only the write state nullable, and [MaybeNull] only the read state.
        var info = context.Create(parameter);
        return info.WriteState == NullabilityState.Nullable || info.ReadState == NullabilityState.Nullable;
    }

}

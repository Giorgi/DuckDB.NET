using DuckDB.NET.Data.DataChunk.Writer;
using System.Linq.Expressions;
using System.Reflection;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionTableFunctionExtensions
{
    extension(DuckDBConnection connection)
    {
        public void RegisterTableFunction<TData, TProjection>(
            string name,
            Func<IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            connection.RegisterTableFunction(name,
                () => new TableFunction(columns, dataFunc()),
                mapper);
        }

        public void RegisterTableFunction<T1, TData, TProjection>(
            string name,
            Func<T1, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var read1 = CompileParameterReader<T1>(0, name);
            connection.RegisterTableFunction<T1>(name,
                parameters => new TableFunction(columns, dataFunc(read1(parameters))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, TData, TProjection>(
            string name,
            Func<T1, T2, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var read1 = CompileParameterReader<T1>(0, name);
            var read2 = CompileParameterReader<T2>(1, name);
            connection.RegisterTableFunction<T1, T2>(name,
                parameters => new TableFunction(columns, dataFunc(read1(parameters), read2(parameters))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, TData, TProjection>(
            string name,
            Func<T1, T2, T3, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var read1 = CompileParameterReader<T1>(0, name);
            var read2 = CompileParameterReader<T2>(1, name);
            var read3 = CompileParameterReader<T3>(2, name);
            connection.RegisterTableFunction<T1, T2, T3>(name,
                parameters => new TableFunction(columns, dataFunc(read1(parameters), read2(parameters), read3(parameters))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, T4, TData, TProjection>(
            string name,
            Func<T1, T2, T3, T4, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var read1 = CompileParameterReader<T1>(0, name);
            var read2 = CompileParameterReader<T2>(1, name);
            var read3 = CompileParameterReader<T3>(2, name);
            var read4 = CompileParameterReader<T4>(3, name);
            connection.RegisterTableFunction<T1, T2, T3, T4>(name,
                parameters => new TableFunction(columns, dataFunc(read1(parameters), read2(parameters), read3(parameters), read4(parameters))),
                mapper);
        }
    }

    private static readonly MethodInfo GetValueMethod = typeof(IDuckDBValueReader).GetMethod(nameof(IDuckDBValueReader.GetValue))!;
    private static readonly MethodInfo WriteValueMethod = typeof(IDuckDBDataWriter).GetMethod(nameof(IDuckDBDataWriter.WriteValue))!;

    private static (ColumnInfo[] columns, Action<object?, IDuckDBDataWriter[], ulong> mapper) ParseProjection<TData, TProjection>(Expression<Func<TData, TProjection>> projection)
    {
        var (names, types, accessors) = projection.Body switch
        {
            NewExpression newExpr => ParseNewExpression(newExpr),
            MemberInitExpression initExpr => ParseMemberInitExpression(initExpr),
            MemberExpression memberExpr => ([memberExpr.Member.Name], [memberExpr.Type], [memberExpr]),
            _ => throw new ArgumentException("Projection must be a new expression, object initializer, or single property access.")
        };

        var columns = new ColumnInfo[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            columns[i] = new ColumnInfo(names[i], types[i]);
        }

        var combinedWriter = CompileCombinedWriter<TData>(names.Length, types, accessors, projection.Parameters[0]);

        return (columns, Mapper);

        void Mapper(object? item, IDuckDBDataWriter[] writers, ulong rowIndex)
        {
            combinedWriter((TData)item!, writers, rowIndex);
        }
    }

    private static (string[] names, Type[] types, Expression[] accessors) ParseNewExpression(NewExpression newExpr)
    {
        var count = newExpr.Arguments.Count;
        var names = new string[count];
        var types = new Type[count];
        var accessors = new Expression[count];

        for (int i = 0; i < count; i++)
        {
            names[i] = newExpr.Members![i].Name;
            types[i] = newExpr.Arguments[i].Type;
            accessors[i] = newExpr.Arguments[i];
        }

        return (names, types, accessors);
    }

    private static (string[] names, Type[] types, Expression[] accessors) ParseMemberInitExpression(MemberInitExpression initExpr)
    {
        var count = initExpr.Bindings.Count;
        var names = new string[count];
        var types = new Type[count];
        var accessors = new Expression[count];

        for (int i = 0; i < count; i++)
        {
            var binding = (MemberAssignment)initExpr.Bindings[i];
            names[i] = binding.Member.Name;
            types[i] = binding.Expression.Type;
            accessors[i] = binding.Expression;
        }

        return (names, types, accessors);
    }

    private static Func<IReadOnlyList<IDuckDBValueReader>, T> CompileParameterReader<T>(int index, string functionName)
    {
        var nullableUnderlyingType = Nullable.GetUnderlyingType(typeof(T));

        if (nullableUnderlyingType is not null)
        {
            var readNullable = CompileNullableReader<T>(nullableUnderlyingType);
            return parameters =>
            {
                var reader = parameters[index];
                return reader.IsNull() ? default! : readNullable(reader);
            };
        }

        return parameters =>
        {
            var reader = parameters[index];
            if (reader.IsNull())
            {
                if (default(T) is null) return default!;
                throw new InvalidOperationException($"Table function '{functionName}' argument {index + 1} is NULL, but parameter type '{typeof(T).Name}' is non-nullable.");
            }
            return reader.GetValue<T>();
        };
    }

    private static Func<IDuckDBValueReader, T> CompileNullableReader<T>(Type underlyingType)
    {
        var readerParam = Expression.Parameter(typeof(IDuckDBValueReader), "reader");

        var getValue = Expression.Call(readerParam, GetValueMethod.MakeGenericMethod(underlyingType));
        var convert = Expression.Convert(getValue, typeof(T));

        return Expression.Lambda<Func<IDuckDBValueReader, T>>(convert, readerParam).Compile();
    }

    private static Action<TData, IDuckDBDataWriter[], ulong> CompileCombinedWriter<TData>(int columnCount, Type[] types, Expression[] accessors, ParameterExpression originalParam)
    {
        var dataParam = Expression.Parameter(typeof(TData), "data");
        var writersParam = Expression.Parameter(typeof(IDuckDBDataWriter[]), "writers");
        var rowIndexParam = Expression.Parameter(typeof(ulong), "rowIndex");

        var replacer = new ParameterReplacer(originalParam, dataParam);
        var writeCalls = new Expression[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            var reboundAccessor = replacer.Visit(accessors[i]);
            var writerAccess = Expression.ArrayIndex(writersParam, Expression.Constant(i));

            writeCalls[i] = Expression.Call(
                writerAccess,
                WriteValueMethod.MakeGenericMethod(types[i]),
                reboundAccessor,
                rowIndexParam);
        }

        var body = Expression.Block(writeCalls);
        return Expression.Lambda<Action<TData, IDuckDBDataWriter[], ulong>>(body, dataParam, writersParam, rowIndexParam).Compile();
    }

    // Rebinds parameter references when embedding sub-expressions from the user's projection lambda
    // into a new lambda with a different parameter. E.g. given (Employee e) => new { e.Id }, the
    // accessor "e.Id" references the original parameter "e". When we compile the combined writer
    // (data, writers, rowIndex) => { writers[0].WriteValue(data.Id, rowIndex); ... }, we must
    // replace "e" with "data".
    private sealed class ParameterReplacer(ParameterExpression oldParam, ParameterExpression newParam) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
            => node == oldParam ? newParam : base.VisitParameter(node);
    }
}

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
            connection.RegisterTableFunction<T1>(name,
                parameters => new TableFunction(columns, dataFunc(parameters[0].GetValue<T1>())),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, TData, TProjection>(
            string name,
            Func<T1, T2, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            connection.RegisterTableFunction<T1, T2>(name,
                parameters => new TableFunction(columns, dataFunc(
                    parameters[0].GetValue<T1>(),
                    parameters[1].GetValue<T2>())),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, TData, TProjection>(
            string name,
            Func<T1, T2, T3, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            connection.RegisterTableFunction<T1, T2, T3>(name,
                parameters => new TableFunction(columns, dataFunc(
                    parameters[0].GetValue<T1>(),
                    parameters[1].GetValue<T2>(),
                    parameters[2].GetValue<T3>())),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, T4, TData, TProjection>(
            string name,
            Func<T1, T2, T3, T4, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            connection.RegisterTableFunction<T1, T2, T3, T4>(name,
                parameters => new TableFunction(columns, dataFunc(
                    parameters[0].GetValue<T1>(),
                    parameters[1].GetValue<T2>(),
                    parameters[2].GetValue<T3>(),
                    parameters[3].GetValue<T4>())),
                mapper);
        }
    }

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
            var columnType = Nullable.GetUnderlyingType(types[i]) ?? types[i];
            columns[i] = new ColumnInfo(names[i], columnType);
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

    private static readonly MethodInfo WriteValueMethod = typeof(IDuckDBDataWriter).GetMethod(nameof(IDuckDBDataWriter.WriteValue))!;

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

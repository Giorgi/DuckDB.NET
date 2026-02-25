using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.DataChunk.Writer;
using System.Linq.Expressions;
using System.Reflection;

namespace DuckDB.NET.Data;

public static class DuckDBConnectionTableFunctionExtensions
{
    private record TableFunctionParameter(string? NamedAs, int? PositionalIndex, Type Type)
    {
        public bool IsNamed => NamedAs is not null;
    }

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

        // High-level table function registration flow (1–4 param overloads):
        //
        // Example: connection.RegisterTableFunction("my_func",
        //     (int count, [Named] string? prefix) => GetEmployees(count).Select(e => ...),
        //     e => new { e.Id, e.Name });
        //
        // 1. AnalyzeParameters inspects the lambda's parameters:
        //      parameters[0] = (NamedAs: null,     PositionalIndex: 0, Type: int)    — positional
        //      parameters[1] = (NamedAs: "prefix", PositionalIndex: null, Type: string) — named
        //
        // 2. CompileValueReader builds a Func<IDuckDBValueReader, T> per parameter —
        //    handles null-checking and type conversion (what to do with the value):
        //      read1: reader → reader.GetValue<int>()         (throws if NULL)
        //      read2: reader → reader.IsNull() ? null : reader.GetValue<string>()
        //
        // 3. Resolve picks the right IDuckDBValueReader from the positional list or
        //    named dictionary (where to find the value):
        //      Resolve(p[0], positional, named) → positional[0]
        //      Resolve(p[1], positional, named) → named["prefix"]
        //
        // 4. The bind lambda wires them together (called once per query):
        //      (positional, named) => {
        //          int count      = read1(Resolve(p[0], positional, named));
        //          string? prefix = read2(Resolve(p[1], positional, named));
        //          return new TableFunction(columns, dataFunc(count, prefix));
        //      }
        //
        // 5. RegisterInternal registers with DuckDB:
        //      1 positional parameter (INTEGER)
        //      1 named parameter ("prefix", VARCHAR)

        public void RegisterTableFunction<T1, TData, TProjection>(
            string name,
            Func<T1, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var parameters = AnalyzeParameters(dataFunc.Method.GetParameters());
            var read1 = CompileValueReader<T1>(parameters[0], name);

            RegisterInternal(connection, name, parameters,
                (positional, named) => new TableFunction(columns, dataFunc(
                    read1(Resolve(parameters[0], positional, named)))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, TData, TProjection>(
            string name,
            Func<T1, T2, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var parameters = AnalyzeParameters(dataFunc.Method.GetParameters());
            
            var read1 = CompileValueReader<T1>(parameters[0], name);
            var read2 = CompileValueReader<T2>(parameters[1], name);

            RegisterInternal(connection, name, parameters,
                (positional, named) => new TableFunction(columns, dataFunc(
                    read1(Resolve(parameters[0], positional, named)),
                    read2(Resolve(parameters[1], positional, named)))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, TData, TProjection>(
            string name,
            Func<T1, T2, T3, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var parameters = AnalyzeParameters(dataFunc.Method.GetParameters());
            
            var read1 = CompileValueReader<T1>(parameters[0], name);
            var read2 = CompileValueReader<T2>(parameters[1], name);
            var read3 = CompileValueReader<T3>(parameters[2], name);

            RegisterInternal(connection, name, parameters,
                (positional, named) => new TableFunction(columns, dataFunc(
                    read1(Resolve(parameters[0], positional, named)),
                    read2(Resolve(parameters[1], positional, named)),
                    read3(Resolve(parameters[2], positional, named)))),
                mapper);
        }

        public void RegisterTableFunction<T1, T2, T3, T4, TData, TProjection>(
            string name,
            Func<T1, T2, T3, T4, IEnumerable<TData>> dataFunc,
            Expression<Func<TData, TProjection>> projection)
        {
            var (columns, mapper) = ParseProjection(projection);
            var parameters = AnalyzeParameters(dataFunc.Method.GetParameters());
            
            var read1 = CompileValueReader<T1>(parameters[0], name);
            var read2 = CompileValueReader<T2>(parameters[1], name);
            var read3 = CompileValueReader<T3>(parameters[2], name);
            var read4 = CompileValueReader<T4>(parameters[3], name);

            RegisterInternal(connection, name, parameters,
                (positional, named) => new TableFunction(columns, dataFunc(
                    read1(Resolve(parameters[0], positional, named)),
                    read2(Resolve(parameters[1], positional, named)),
                    read3(Resolve(parameters[2], positional, named)),
                    read4(Resolve(parameters[3], positional, named)))),
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

    private static Func<IDuckDBValueReader, T> CompileValueReader<T>(TableFunctionParameter param, string functionName)
    {
        var nullableUnderlyingType = Nullable.GetUnderlyingType(typeof(T));

        if (nullableUnderlyingType is not null)
        {
            var readNullable = CompileNullableReader<T>(nullableUnderlyingType);
            return reader => reader.IsNull() ? default! : readNullable(reader);
        }

        var errorContext = param.IsNamed
            ? $"named parameter '{param.NamedAs}'"
            : $"argument {param.PositionalIndex!.Value + 1}";

        return reader =>
        {
            if (reader.IsNull())
            {
                if (default(T) is null) return default!;
                throw new InvalidOperationException(
                    $"Table function '{functionName}' {errorContext} is NULL, "
                    + $"but parameter type '{typeof(T).Name}' is non-nullable.");
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

    private static IDuckDBValueReader Resolve(TableFunctionParameter param, IReadOnlyList<IDuckDBValueReader> positional, IReadOnlyDictionary<string, IDuckDBValueReader> named)
        => param.IsNamed ? named[param.NamedAs!] : positional[param.PositionalIndex!.Value];

    private static void RegisterInternal(DuckDBConnection connection, string name, TableFunctionParameter[] parameters,
                                         Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind,
                                         Action<object?, IDuckDBDataWriter[], ulong> mapper)
    {
        var positionalTypes = new List<DuckDBLogicalType>();
        var namedDefinitions = new List<NamedParameterDefinition>();

        foreach (var param in parameters)
        {
            if (param.IsNamed)
                namedDefinitions.Add(new(param.NamedAs!, param.Type));
            else
                positionalTypes.Add(param.Type.GetLogicalType());
        }

        connection.RegisterTableFunctionInternal(name, bind, mapper, positionalTypes.ToArray(), namedDefinitions.ToArray());
    }

    private static TableFunctionParameter[] AnalyzeParameters(ParameterInfo[] methodParams)
    {
        var result = new TableFunctionParameter[methodParams.Length];
        int nextPositional = 0;

        for (int i = 0; i < methodParams.Length; i++)
        {
            var attr = methodParams[i].GetCustomAttribute<NamedAttribute>();
            result[i] = attr is not null
                ? new(attr.Name ?? methodParams[i].Name!, null, methodParams[i].ParameterType)
                : new(null, nextPositional++, methodParams[i].ParameterType);
        }

        return result;
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

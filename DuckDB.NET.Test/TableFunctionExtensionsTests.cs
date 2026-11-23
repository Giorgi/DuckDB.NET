using System.Globalization;
#nullable enable

namespace DuckDB.NET.Test;

record Employee(int Id, string Name, double Salary);

class EmployeeDto
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public class TableFunctionExtensionsTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private static IEnumerable<Employee> GetEmployees(int count) =>
        Enumerable.Range(1, count).Select(i => new Employee(i, $"Employee{i}", 50000 + i * 100));

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedZeroParams()
    {
        var employees = new[]
        {
            new Employee(1, "Alice", 60000),
            new Employee(2, "Bob", 70000),
        };

        Connection.RegisterTableFunction("ext_zero",
            () => employees.AsEnumerable(),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_zero();").ToList();

        data.Should().BeEquivalentTo([(1, "Alice"), (2, "Bob")]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedOneParam()
    {
        Connection.RegisterTableFunction("ext_one",
            (int count) => GetEmployees(count),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_one(3);").ToList();

        data.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2"), (3, "Employee3")]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedTwoParams()
    {
        Connection.RegisterTableFunction("ext_two",
            (int start, int count) => Enumerable.Range(start, count).Select(i => new Employee(i, $"E{i}", i * 1000)),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>("SELECT * FROM ext_two(10, 3);").ToList();

        data.Select(d => d.Item1).Should().BeEquivalentTo([10, 11, 12]);
        data.Select(d => d.Item2).Should().BeEquivalentTo(["E10", "E11", "E12"]);
        data.Select(d => d.Item3).Should().BeEquivalentTo([10000.0, 11000.0, 12000.0]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedComputedColumns()
    {
        Connection.RegisterTableFunction("ext_computed",
            (int count) => GetEmployees(count),
            e => new { FullName = "Dr. " + e.Name, DoubleSalary = e.Salary * 2 });

        var data = Connection.Query<(string, double)>("SELECT * FROM ext_computed(2);").ToList();

        data.Should().BeEquivalentTo([("Dr. Employee1", 100200.0), ("Dr. Employee2", 100400.0)]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedMemberInit()
    {
        Connection.RegisterTableFunction("ext_init",
            (int count) => GetEmployees(count),
            e => new EmployeeDto { Id = e.Id, Name = e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_init(2);").ToList();

        data.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedLargeResultSet()
    {
        Connection.RegisterTableFunction("ext_large",
            (int count) => GetEmployees(count),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_large(5000);").ToList();

        data.Should().HaveCount(5000);
        data.First().Should().Be((1, "Employee1"));
        data.Last().Should().Be((5000, "Employee5000"));
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedAsyncEnumerable()
    {
        Connection.RegisterTableFunction("ext_async",
            (int count) => FetchEmployeesAsync(count).ToBlockingEnumerable(),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_async(5);").ToList();

        data.Should().BeEquivalentTo([
            (1, "Employee1"), (2, "Employee2"), (3, "Employee3"),
            (4, "Employee4"), (5, "Employee5")
        ]);

        static async IAsyncEnumerable<Employee> FetchEmployeesAsync(int count)
        {
            for (int i = 1; i <= count; i++)
            {
                await Task.Delay(10); // simulate async I/O
                yield return new Employee(i, $"Employee{i}", 50000 + i * 100);
            }
        }
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedThreeParams()
    {
        Connection.RegisterTableFunction("ext_three",
            (int start, int count, string prefix) => Enumerable.Range(start, count).Select(i => new Employee(i, $"{prefix}{i}", i * 1000)),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>("SELECT * FROM ext_three(5, 3, 'Emp');").ToList();

        data.Should().BeEquivalentTo([(5, "Emp5", 5000.0), (6, "Emp6", 6000.0), (7, "Emp7", 7000.0)]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedFourParams()
    {
        Connection.RegisterTableFunction("ext_four",
            (int start, int count, string prefix, double multiplier) =>
                Enumerable.Range(start, count).Select(i => new Employee(i, $"{prefix}{i}", i * multiplier)),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>("SELECT * FROM ext_four(1, 2, 'X', 1.5);").ToList();

        data.Should().BeEquivalentTo([(1, "X1", 1.5), (2, "X2", 3.0)]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedNullableProjectionColumn()
    {
        var employees = new[]
        {
            new Employee(1, "Alice", 60000),
            new Employee(2, "Bob", 70000),
            new Employee(3, "Cara", 80000),
        };

        Connection.RegisterTableFunction("ext_nullable_col",
            () => employees.AsEnumerable(),
            e => new { MaybeId = e.Id % 2 == 0 ? (int?)e.Id : null });

        var data = Connection.Query<int?>("SELECT * FROM ext_nullable_col();").ToList();

        data.Should().BeEquivalentTo(new int?[] { null, 2, null });
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedNullIntParameter()
    {
        Connection.RegisterTableFunction("ext_null_int",
            (int value) => new[] { new Employee(value, "X", 1) },
            e => new { e.Id });

        Connection.Invoking(con => con.Query<int>("SELECT * FROM ext_null_int(NULL::INTEGER);"))
                  .Should().Throw<DuckDBException>().WithMessage("*Table function 'ext_null_int' argument 1 is NULL, but parameter type 'Int32' is non-nullable.*");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedNullStringParameter()
    {
        Connection.RegisterTableFunction("ext_null_string",
            (string value) => new[] { new Employee(1, value ?? "default", 1) },
            e => new { e.Name });

        var data = Connection.Query<string>("SELECT * FROM ext_null_string(NULL::VARCHAR);").Single();
        data.Should().Be("default");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedNullableIntParameter()
    {
        Connection.RegisterTableFunction("ext_null_nullable",
            (int? value) => new[] { new Employee(value ?? -1, "N", 1) },
            e => new { e.Id });

        var nonNull = Connection.Query<int>("SELECT * FROM ext_null_nullable(5);").Single();
        nonNull.Should().Be(5);

        var withNull = Connection.Query<int>("SELECT * FROM ext_null_nullable(NULL::INTEGER);").Single();
        withNull.Should().Be(-1);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionNamedParameter()
    {
        Connection.RegisterTableFunction("ext_named", (int count, [Named(Default = null)] string? prefix) =>
                GetEmployees(count).Select(e => e with { Name = (prefix ?? "") + e.Name }),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_named(2, prefix = 'Dr. ');").ToList();
        data.Should().BeEquivalentTo([(1, "Dr. Employee1"), (2, "Dr. Employee2")]);

        var data2 = Connection.Query<(int, string)>("SELECT * FROM ext_named(2);").ToList();
        data2.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);

        // Explicit NULL should behave the same as omitted
        var data3 = Connection.Query<(int, string)>("SELECT * FROM ext_named(2, prefix = NULL);").ToList();
        data3.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionMultipleNamedParameters()
    {
        Connection.RegisterTableFunction("ext_multi_named",
            (int count, [Named(Default = null)] string? prefix, [Named(Default = null)] double? multiplier) =>
                GetEmployees(count).Select(e => e with
                {
                    Name = (prefix ?? "") + e.Name,
                    Salary = e.Salary * (multiplier ?? 1)
                }),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>(
            "SELECT * FROM ext_multi_named(2, prefix = 'X', multiplier = 2.0);").ToList();
        data.Should().BeEquivalentTo([(1, "XEmployee1", 100200.0), (2, "XEmployee2", 100400.0)]);

        // Provide prefix but omit multiplier
        var data2 = Connection.Query<(int, string, double)>(
            "SELECT * FROM ext_multi_named(2, prefix = 'Y');").ToList();
        data2.Should().BeEquivalentTo([(1, "YEmployee1", 50100.0), (2, "YEmployee2", 50200.0)]);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionNamedParameterCustomName()
    {
        Connection.RegisterTableFunction("ext_custom_name", (int count, [Named("max_rows")] int? limit) => GetEmployees(limit ?? count), e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_custom_name(10, max_rows = 2);").ToList();
        data.Should().HaveCount(2);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionNamedParameterNonNullableThrows()
    {
        Connection.RegisterTableFunction("ext_named_nonnull",
            (int count, [Named] int limit) => GetEmployees(count).Take(limit),
            e => new { e.Id, e.Name });

        // A named parameter without a default is required: DuckDB refuses the call before the function runs.
        Connection.Invoking(c => c.Query<(int, string)>("SELECT * FROM ext_named_nonnull(5);"))
            .Should().Throw<DuckDBException>()
            .WithMessage("*Required parameter \"limit\" is missing*");

        Connection.Invoking(c => c.Query<(int, string)>("SELECT * FROM ext_named_nonnull(5, \"limit\" = NULL);"))
            .Should().Throw<DuckDBException>()
            .WithMessage("*named parameter 'limit' is NULL*non-nullable*");
    }

    // Nullable or not, a named parameter without a default is required: a forgotten argument is an error, not NULL.
    [Fact]
    [Trait("V2", "Ready")]
    public void ANullableNamedParameterWithoutADefaultIsRequired()
    {
        Connection.RegisterTableFunction("ext_named_required", (int count, [Named] string? prefix) =>
                GetEmployees(count).Select(e => e with { Name = (prefix ?? "none: ") + e.Name }),
            e => new { e.Id, e.Name });

        Connection.Invoking(c => c.Query<(int, string)>("SELECT * FROM ext_named_required(1);"))
            .Should().Throw<DuckDBException>()
            .WithMessage("*Required parameter \"prefix\" is missing*");

        Connection.Query<(int, string)>("SELECT * FROM ext_named_required(1, prefix = NULL);").Should().Equal((1, "none: Employee1"));
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void NamedParametersTakeTheirDefaults()
    {
        Connection.RegisterTableFunction("ext_defaults",
            ([Named(Default = 2)] int count, [Named(Default = "Mr. ")] string prefix, [Named(Default = "2024-01-31")] DateOnly from, [Named(Default = "12.5")] decimal rate) =>
                GetEmployees(count).Select(e => e with { Name = $"{prefix}{e.Name} {from:yyyy-MM-dd} {rate == 12.5m}" }),
            e => new { e.Id, e.Name });

        Connection.Query<(int, string)>("SELECT * FROM ext_defaults();").Should().Equal(
            (1, "Mr. Employee1 2024-01-31 True"), (2, "Mr. Employee2 2024-01-31 True"));

        Connection.Query<(int, string)>("SELECT * FROM ext_defaults(count = 1, prefix = 'Dr. ');").Should().Equal(
            (1, "Dr. Employee1 2024-01-31 True"));

        Connection.RegisterTableFunction("ext_defaults_text",
            (int count, [Named(Default = "00000000-0000-0000-0000-000000000001")] Guid id, [Named(Default = "12:30:00")] TimeOnly starts) =>
                GetEmployees(count).Select(e => e with { Name = $"{id} {starts:HH:mm}" }),
            e => new { e.Id, e.Name });

        Connection.Query<(int, string)>("SELECT * FROM ext_defaults_text(1);").Should().Equal((1, "00000000-0000-0000-0000-000000000001 12:30"));
        Connection.Query<(int, string)>("SELECT * FROM ext_defaults_text(1, starts = TIME '08:05:00');").Should().Equal((1, "00000000-0000-0000-0000-000000000001 08:05"));
    }

    // count(*) reads no column. With projection pushdown the output chunk may then have no vector to set the size of.
    [Fact]
    [Trait("V2", "Ready")]
    public void AQueryThatReadsNoColumnCountsTheRows()
    {
        Connection.RegisterTableFunction("ext_count_only", (int count) => GetEmployees(count), e => new { e.Id, e.Name });

        Connection.QuerySingle<long>("SELECT count(*) FROM ext_count_only(5000)").Should().Be(5000);
        Connection.QuerySingle<long>("SELECT count(*) FROM ext_count_only(0)").Should().Be(0);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void ADefaultThatIsNotOfTheParameterTypeIsRefusedAtRegistration()
    {
        Connection.Invoking(c => c.RegisterTableFunction("ext_bad_default", ([Named(Default = "not a date")] DateOnly from) => GetEmployees(1), e => new { e.Id }))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*'not a date'*'from'*ext_bad_default*");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void ACSharpDefaultOnANamedParameterIsRefused()
    {
#pragma warning disable CS9099 // The default value is what the test is about.
        Connection.Invoking(c => c.RegisterTableFunction("ext_csharp_default", (int count, [Named] int limit = 3) => GetEmployees(count).Take(limit), e => new { e.Id }))
#pragma warning restore CS9099
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*'limit'*[Named(Default = ...)]*");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_ZeroSqlParams()
    {
        IReadOnlyList<ProjectedColumn> captured = [];

        Connection.RegisterTableFunction("ext_proj_zero",
            (IReadOnlyList<ProjectedColumn> projected) =>
            {
                captured = projected;
                return GetEmployees(3);
            },
            e => new { e.Id, e.Name, e.Salary });

        var names = Connection.Query<string>("SELECT name FROM ext_proj_zero();").ToList();
        names.Should().Equal("Employee1", "Employee2", "Employee3");

        captured.Should().HaveCount(1);
        captured[0].Index.Should().Be(1);
        captured[0].Name.Should().Be("Name");
        captured[0].Type.Should().Be(typeof(string));
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_OneSqlParam()
    {
        IReadOnlyList<ProjectedColumn> captured = [];

        Connection.RegisterTableFunction("ext_proj_one",
            (IReadOnlyList<ProjectedColumn> projected, int count) =>
            {
                captured = projected;
                return GetEmployees(count);
            },
            e => new { e.Id, e.Name, e.Salary });

        var ids = Connection.Query<int>("SELECT id FROM ext_proj_one(5);").ToList();
        ids.Should().Equal(1, 2, 3, 4, 5);

        captured.Select(p => p.Index).Should().Equal(0);
        captured[0].Name.Should().Be("Id");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_MixedPositionalAndNamed()
    {
        IReadOnlyList<ProjectedColumn> captured = [];

        Connection.RegisterTableFunction("ext_proj_mixed",
            (IReadOnlyList<ProjectedColumn> projected, int start, [Named] int? take) =>
            {
                captured = projected;
                return Enumerable.Range(start, take ?? 3).Select(i => new Employee(i, $"E{i}", i * 100));
            },
            e => new { e.Id, e.Name, e.Salary });

        var results = Connection.Query<(int, string)>("SELECT id, name FROM ext_proj_mixed(10, take => 2);").ToList();
        results.Should().Equal((10, "E10"), (11, "E11"));

        captured.Select(p => p.Index).Should().Equal(0, 1);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_SourceSideNarrowing()
    {
        // Factory records which columns it fetched, simulating a narrow remote read.
        var fetchLog = new List<string>();

        Connection.RegisterTableFunction("ext_proj_narrow",
            (IReadOnlyList<ProjectedColumn> projected, int count) =>
            {
                fetchLog.Add(string.Join(",", projected.Select(p => p.Name)));
                return GetEmployees(count);
            },
            e => new { e.Id, e.Name, e.Salary });

        Connection.Query<string>("SELECT name FROM ext_proj_narrow(3);").ToList();
        Connection.Query<(int, double)>("SELECT id, salary FROM ext_proj_narrow(3);").ToList();
        Connection.Query<(int, string, double)>("SELECT * FROM ext_proj_narrow(3);").ToList();

        fetchLog.Should().Equal("Name", "Id,Salary", "Id,Name,Salary");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_MisplacedRejected()
    {
        var act = () => Connection.RegisterTableFunction("ext_proj_misplaced",
            (int count, IReadOnlyList<ProjectedColumn> projected) => GetEmployees(count),
            e => new { e.Id, e.Name });

        act.Should().Throw<InvalidOperationException>().WithMessage("*must be the first parameter*");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_NamedOnProjectionRejected()
    {
        var act = () => Connection.RegisterTableFunction("ext_proj_named",
            ([Named] IReadOnlyList<ProjectedColumn> projected, int count) => GetEmployees(count),
            e => new { e.Id, e.Name });

        act.Should().Throw<InvalidOperationException>().WithMessage("*[Named]*ProjectedColumn*");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedRecursiveCte()
    {
        // Regression test for https://github.com/Giorgi/DuckDB.NET/issues/348
        // A recursive CTE re-initializes the table function scan on every iteration
        // without re-binding, so each scan must enumerate the data from the beginning.
        Connection.RegisterTableFunction("ext_recursive",
            () => GetEmployees(5),
            e => new { e.Id });

        var data = Connection.Query<int>("""
            WITH RECURSIVE r AS (
                SELECT 1 AS x
                UNION ALL
                SELECT n.Id FROM r JOIN ext_recursive() n ON n.Id = r.x + 1
            )
            SELECT x FROM r ORDER BY x;
            """).ToList();

        data.Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionProjection_FactoryThrows_InnerExceptionPreserved()
    {
        var originalException = new NotSupportedException("custom factory error");
        Func<IReadOnlyList<ProjectedColumn>, int, IEnumerable<Employee>> dataFunc = (projected, count) => throw originalException;

        Connection.RegisterTableFunction("ext_proj_throws", dataFunc, e => new { e.Id, e.Name });

        var act = () => Connection.Query<int>("SELECT id FROM ext_proj_throws(1);").ToList();
        var ex = act.Should().Throw<DuckDBException>().Which;

        ex.Message.Should().Contain("custom factory error");
        ex.InnerException.Should().BeSameAs(originalException);
    }

    record EmployeeName(int Id, string Name);

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedConstructorCallRejected()
    {
        var employees = GetEmployees(3);

        // The arguments of a constructor have no column names. It used to fail with NullReferenceException.
        Connection.Invoking(connection => connection.RegisterTableFunction("ext_ctor_record", () => employees, e => new EmployeeName(e.Id, e.Name)))
            .Should().Throw<ArgumentException>().WithMessage("*no column names*");

        Connection.Invoking(connection => connection.RegisterTableFunction("ext_ctor_tuple", () => employees, e => new Tuple<int, string>(e.Id, e.Name)))
            .Should().Throw<ArgumentException>().WithMessage("*no column names*");

        Connection.Invoking(connection => connection.RegisterTableFunction("ext_ctor_empty", () => employees, e => new EmployeeDto()))
            .Should().Throw<ArgumentException>().WithMessage("*no column names*");
    }

    private static int idReads;
    private static int nameReads;
    private static int salaryReads;

    private static int CountedId(Employee employee)
    {
        System.Threading.Interlocked.Increment(ref idReads);
        return employee.Id;
    }

    private static string CountedName(Employee employee)
    {
        System.Threading.Interlocked.Increment(ref nameReads);
        return employee.Name;
    }

    private static double CountedSalary(Employee employee)
    {
        System.Threading.Interlocked.Increment(ref salaryReads);
        return employee.Salary;
    }

    // The projection is compiled into a method that writes a row, for the columns that the query reads. The value
    // of a column that the query does not read is not asked for.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsEachColumnOnceForARow()
    {
        const int rowCount = 5000;

        Connection.RegisterTableFunction("ext_counted",
            () => GetEmployees(rowCount),
            e => new { Id = CountedId(e), Name = CountedName(e), Salary = CountedSalary(e) });

        (idReads, nameReads, salaryReads) = (0, 0, 0);

        // The columns in another order than the projection has them, over more rows than one chunk holds.
        var rows = Connection.Query<(double Salary, int Id, string Name)>("SELECT Salary, Id, Name FROM ext_counted() ORDER BY Id").ToList();

        rows.Should().HaveCount(rowCount);
        rows.Should().OnlyContain(row => row.Name == $"Employee{row.Id}" && row.Salary == 50000 + row.Id * 100);
        (idReads, nameReads, salaryReads).Should().Be((rowCount, rowCount, rowCount));

        (idReads, nameReads, salaryReads) = (0, 0, 0);

        // One column. The other two are not read by the query, so nothing asks for their values.
        var names = Connection.Query<string>("SELECT Name FROM ext_counted()").ToList();

        names.Should().HaveCount(rowCount);
        names.Should().OnlyContain(name => name.StartsWith("Employee"));
        nameReads.Should().Be(rowCount);
        idReads.Should().Be(0);
        salaryReads.Should().Be(0);

        (idReads, nameReads, salaryReads) = (0, 0, 0);

        // The same columns again in a second query, and another set of columns after it: each set has its own
        // compiled method, and the method of a set is found again.
        Connection.Query<string>("SELECT Name FROM ext_counted()").Should().HaveCount(rowCount);
        Connection.QuerySingle<long>("SELECT sum(Id)::BIGINT FROM ext_counted()").Should().Be((long)rowCount * (rowCount + 1) / 2);

        nameReads.Should().Be(rowCount);
        idReads.Should().Be(rowCount);
        salaryReads.Should().Be(0);
    }

    private struct Measurement
    {
        public int Id { get; init; }
        public long Value { get; init; }
    }

    private static IEnumerable<T> Enumerate<T>(T[] rows)
    {
        foreach (var row in rows)
        {
            yield return row;
        }
    }

    // An array and a List are read by position, every other source through its enumerator, and the rows keep their
    // type on the way, class or struct. More rows than two chunks hold, so that a scan that loses its place between
    // chunks shows. Each function is also scanned twice in one query: both scans have to start at the first row.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsEverySourceTheSameWay()
    {
        const int rowCount = 5000;

        var employees = Enumerable.Range(0, rowCount).Select(i => new Employee(i, "Employee" + i, i * 2.5)).ToArray();
        var measurements = Enumerable.Range(0, rowCount).Select(i => new Measurement { Id = i, Value = i * 3L }).ToArray();

        Connection.RegisterTableFunction("src_class_array", () => employees, e => new { e.Id, e.Name });
        Connection.RegisterTableFunction("src_class_list", () => employees.ToList(), e => new { e.Id, e.Name });
        Connection.RegisterTableFunction("src_class_iterator", () => Enumerate(employees), e => new { e.Id, e.Name });
        Connection.RegisterTableFunction("src_class_set", () => new HashSet<Employee>(employees), e => new { e.Id, e.Name });

        Connection.RegisterTableFunction("src_struct_array", () => measurements, m => new { m.Id, m.Value });
        Connection.RegisterTableFunction("src_struct_list", () => measurements.ToList(), m => new { m.Id, m.Value });
        Connection.RegisterTableFunction("src_struct_iterator", () => Enumerate(measurements), m => new { m.Id, m.Value });

        var expectedIds = (long)rowCount * (rowCount - 1) / 2;

        foreach (var function in new[] { "src_class_array", "src_class_list", "src_class_iterator", "src_class_set" })
        {
            // Both columns, in the order of the rows for the three sources that have one.
            var rows = Connection.Query<(int Id, string Name)>($"SELECT Id, Name FROM {function}() ORDER BY Id").ToList();
            rows.Should().HaveCount(rowCount, function);
            rows.Should().OnlyContain(row => row.Name == "Employee" + row.Id, function);
            rows.Select(row => row.Id).Should().BeInAscendingOrder(function);

            // One column: the other is not read, so the scan has no writer for it and writes nothing for it.
            Connection.QuerySingle<long>($"SELECT sum(Id)::BIGINT FROM {function}()").Should().Be(expectedIds, function);

            Connection.QuerySingle<long>($"SELECT count(*) FROM {function}() a JOIN {function}() b USING (Id)").Should().Be(rowCount, function);
        }

        foreach (var function in new[] { "src_struct_array", "src_struct_list", "src_struct_iterator" })
        {
            var sums = Connection.QuerySingle<(long Count, long Ids, long Values)>($"SELECT count(*), sum(Id)::BIGINT, sum(Value)::BIGINT FROM {function}()");
            sums.Should().Be(((long)rowCount, expectedIds, expectedIds * 3), function);

            Connection.QuerySingle<long>($"SELECT sum(Value)::BIGINT FROM {function}()").Should().Be(expectedIds * 3, function);
            Connection.QuerySingle<long>($"SELECT count(*) FROM {function}() a JOIN {function}() b USING (Id)").Should().Be(rowCount, function);
        }

        // The rows of an array come back in the order of the array.
        Connection.Query<int>("SELECT Id FROM src_struct_array()").Should().Equal(Enumerable.Range(0, rowCount));
    }

    private static List<int> changingRows = [];

    private static int AddARowAt(int row, int at)
    {
        if (row == at)
        {
            changingRows.Add(-1);
        }

        return row;
    }

    private static int RemoveARowAt(int row, int at)
    {
        if (row == at)
        {
            changingRows.RemoveAt(changingRows.Count - 1);
        }

        return row;
    }

    // A List is read by position, so nothing in the list itself notices a row that is added or removed while the
    // function is read, and the rows of the chunks that follow would shift. The scan compares the length of the
    // list for every chunk and refuses one that changed, as the enumerator of the list did.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedRefusesAListThatChangesLength()
    {
        Connection.RegisterTableFunction("list_grows", () => changingRows, row => new { Value = AddARowAt(row, 100) });
        Connection.RegisterTableFunction("list_shrinks", () => changingRows, row => new { Value = RemoveARowAt(row, 100) });
        Connection.RegisterTableFunction("list_unchanged", () => changingRows, row => new { Value = row });

        foreach (var function in new[] { "list_grows", "list_shrinks" })
        {
            changingRows = Enumerable.Range(0, 5000).ToList();

            Connection.Invoking(connection => connection.Query<int>($"SELECT Value FROM {function}()").ToList())
                .Should().Throw<DuckDBException>().WithMessage("*the rows of a table function cannot be added or removed*");

            // A query that needs the first chunk only: the list is compared after a chunk too, because DuckDB
            // does not ask for another one.
            changingRows = Enumerable.Range(0, 5000).ToList();

            Connection.Invoking(connection => connection.Query<int>($"SELECT Value FROM {function}() LIMIT 150").ToList())
                .Should().Throw<DuckDBException>().WithMessage("*the rows of a table function cannot be added or removed*");
        }

        changingRows = Enumerable.Range(0, 5000).ToList();
        Connection.Query<int>("SELECT Value FROM list_unchanged()").Should().Equal(Enumerable.Range(0, 5000));
    }

    private class Animal
    {
        public string Name { get; init; } = "";
    }

    private sealed class Dog : Animal;

    private interface IShape
    {
        double Area { get; }
    }

    private readonly record struct Square(double Side) : IShape
    {
        public double Area => Side * Side;
    }

    // The type of the rows is the type that the function was registered with. The rows themselves can be of a
    // class derived from it, in an array of the derived class, values that implement an interface, or nullable
    // values.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsRowsOfADerivedOrNullableType()
    {
        Dog[] dogs = [new Dog { Name = "Rex" }, new Dog { Name = "Bella" }];
        Measurement?[] measurements = [new Measurement { Id = 1, Value = 10 }, null, new Measurement { Id = 3, Value = 30 }];

        Connection.RegisterTableFunction("rows_derived_array", () => (IEnumerable<Animal>)dogs, animal => new { animal.Name });
        Connection.RegisterTableFunction("rows_derived_list", () => (IEnumerable<Animal>)dogs.ToList(), animal => new { animal.Name });
        Connection.RegisterTableFunction("rows_interface", () => new IShape[] { new Square(2), new Square(3) }, shape => new { shape.Area });
        Connection.RegisterTableFunction("rows_nullable_int", () => new int?[] { 1, null, 3 }, value => new { Value = value });
        Connection.RegisterTableFunction("rows_nullable_struct", () => measurements.ToList(), m => new { Id = m.HasValue ? m.Value.Id : (int?)null });

        Connection.Query<string>("SELECT Name FROM rows_derived_array()").Should().Equal("Rex", "Bella");
        Connection.Query<string>("SELECT Name FROM rows_derived_list()").Should().Equal("Rex", "Bella");
        Connection.Query<double>("SELECT Area FROM rows_interface()").Should().Equal(4, 9);
        Connection.Query<int?>("SELECT Value FROM rows_nullable_int()").Should().Equal(1, null, 3);
        Connection.Query<int?>("SELECT Id FROM rows_nullable_struct()").Should().Equal(1, null, 3);
    }

    // The sizes at which a scan ends a chunk: no row, a chunk that is one row short, full, and one row over, and
    // two full chunks. An array and a List keep a position of their own between chunks.
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2047)]
    [InlineData(2048)]
    [InlineData(2049)]
    [InlineData(4096)]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsEveryRowAtTheEdgeOfAChunk(int rowCount)
    {
        var rows = Enumerable.Range(0, rowCount).ToArray();

        Connection.RegisterTableFunction($"edge_array_{rowCount}", () => rows, row => new { Value = row });
        Connection.RegisterTableFunction($"edge_list_{rowCount}", () => rows.ToList(), row => new { Value = row });
        Connection.RegisterTableFunction($"edge_iterator_{rowCount}", () => Enumerate(rows), row => new { Value = row });

        foreach (var function in new[] { $"edge_array_{rowCount}", $"edge_list_{rowCount}", $"edge_iterator_{rowCount}" })
        {
            Connection.Query<int>($"SELECT Value FROM {function}()").Should().Equal(rows, function);
        }
    }

    // A recursive query starts the scan of the function again for every round, without binding it again. An
    // array and a List are read by a position that the scan keeps, so each scan has to start at the first row.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedRecursiveCteOverAnArrayAndAList()
    {
        var employees = GetEmployees(5).ToArray();

        Connection.RegisterTableFunction("ext_recursive_array", () => employees, e => new { e.Id });
        Connection.RegisterTableFunction("ext_recursive_list", () => employees.ToList(), e => new { e.Id });

        foreach (var function in new[] { "ext_recursive_array", "ext_recursive_list" })
        {
            var data = Connection.Query<int>($"""
                WITH RECURSIVE r AS (
                    SELECT 1 AS x
                    UNION ALL
                    SELECT n.Id FROM r JOIN {function}() n ON n.Id = r.x + 1
                )
                SELECT x FROM r ORDER BY x;
                """).ToList();

            data.Should().Equal([1, 2, 3, 4, 5], function);
        }
    }

    // Rows that count how often they are enumerated and how often an enumerator is disposed.
    private sealed class TrackedRows(int count, int failAt = -1) : IEnumerable<int>
    {
        public int Count { get; } = count;
        public int FailAt { get; } = failAt;
        public int Created { get; private set; }
        public int Disposed { get; private set; }

        public IEnumerator<int> GetEnumerator()
        {
            Created++;
            return new Enumerator(this);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        private sealed class Enumerator(TrackedRows rows) : IEnumerator<int>
        {
            public int Current { get; private set; } = -1;

            object System.Collections.IEnumerator.Current => Current;

            public bool MoveNext()
            {
                if (Current + 1 == rows.FailAt)
                {
                    throw new InvalidOperationException("the source failed");
                }

                return ++Current < rows.Count;
            }

            public void Reset() => throw new NotSupportedException();

            public void Dispose() => rows.Disposed++;
        }
    }

    private static int FailAt(int row, int at) => row == at ? throw new InvalidOperationException("the projection failed") : row;

    // The enumerator of the rows belongs to a scan and is disposed with it: when the scan has read every row,
    // when the query needs no more rows, and when the source or the projection throws.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedDisposesTheEnumeratorOfEveryScan()
    {
        var rows = new TrackedRows(5000);
        var failingSource = new TrackedRows(5000, failAt: 3000);
        var failingProjection = new TrackedRows(5000);

        Connection.RegisterTableFunction("tracked", () => rows, row => new { Value = row });
        Connection.RegisterTableFunction("tracked_source_fails", () => failingSource, row => new { Value = row });
        Connection.RegisterTableFunction("tracked_projection_fails", () => failingProjection, row => new { Value = FailAt(row, 3000) });

        Connection.QuerySingle<long>("SELECT count(*) FROM tracked()").Should().Be(5000);
        (rows.Created, rows.Disposed).Should().Be((1, 1));

        Connection.Query<int>("SELECT Value FROM tracked() LIMIT 1").Should().HaveCount(1);
        (rows.Created, rows.Disposed).Should().Be((2, 2));

        Connection.QuerySingle<long>("SELECT count(*) FROM tracked() a JOIN tracked() b USING (Value)").Should().Be(5000);
        (rows.Created, rows.Disposed).Should().Be((4, 4));

        Connection.Invoking(connection => connection.Query<int>("SELECT Value FROM tracked_source_fails()").ToList())
            .Should().Throw<DuckDBException>().WithMessage("*the source failed*");
        (failingSource.Created, failingSource.Disposed).Should().Be((1, 1));

        Connection.Invoking(connection => connection.Query<int>("SELECT Value FROM tracked_projection_fails()").ToList())
            .Should().Throw<DuckDBException>().WithMessage("*the projection failed*");
        (failingProjection.Created, failingProjection.Disposed).Should().Be((1, 1));
    }

    // A query that reads no column of the function, such as count(*). DuckDB asks for the first column then, so
    // the expression of the first column is the one that is evaluated, and the others are not.
    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedEvaluatesTheFirstColumnForAQueryThatReadsNone()
    {
        const int rowCount = 5000;

        Connection.RegisterTableFunction("ext_counted_none",
            () => GetEmployees(rowCount),
            e => new { Id = CountedId(e), Name = CountedName(e), Salary = CountedSalary(e) });

        (idReads, nameReads, salaryReads) = (0, 0, 0);

        Connection.QuerySingle<long>("SELECT count(*) FROM ext_counted_none()").Should().Be(rowCount);

        idReads.Should().Be(rowCount);
        nameReads.Should().Be(0);
        salaryReads.Should().Be(0);
    }

    // A timestamp argument read as DateTimeOffset is the instant at UTC, as the column reader gives it, whatever the
    // offset the literal was written with. The parameter is TIMESTAMPTZ, so DuckDB casts a TIMESTAMP, TIMESTAMP_S or
    // TIMESTAMP_MS argument to it; it does not cast TIMESTAMP_NS. DuckDBValueGetValueTests reads every timestamp type.
    [Theory]
    [InlineData("TIMESTAMPTZ '2024-02-29 13:45:30.123456+02'", "2024-02-29T11:45:30.1234560")]
    [InlineData("TIMESTAMP '2024-02-29 13:45:30.123456'", "2024-02-29T13:45:30.1234560")]
    [InlineData("TIMESTAMP_S '2024-02-29 13:45:30'", "2024-02-29T13:45:30.0000000")]
    [InlineData("TIMESTAMP_MS '2024-02-29 13:45:30.123'", "2024-02-29T13:45:30.1230000")]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsATimestampAsDateTimeOffset(string argument, string expectedUtc)
    {
        var expected = DateTime.Parse(expectedUtc, System.Globalization.CultureInfo.InvariantCulture);

        // Its own connection, set to UTC: DuckDB casts a timestamp without a zone to TIMESTAMPTZ in the session's zone,
        // which is otherwise the zone of the machine.
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        connection.Execute("SET TimeZone = 'UTC'");

        connection.RegisterTableFunction("ext_instant",
            (DateTimeOffset at) => new[] { at },
            at => new { UtcTicks = at.UtcTicks, OffsetMinutes = (int)at.Offset.TotalMinutes });

        // Ticks, because a DateTime result column keeps only microseconds.
        var row = connection.QuerySingle<(long, int)>($"SELECT * FROM ext_instant({argument})");

        row.Should().Be((expected.Ticks, 0));
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void RegisterTableFunctionSimplifiedReadsANullableDateTimeOffset()
    {
        Connection.RegisterTableFunction("ext_instant_nullable",
            (DateTimeOffset? at) => new[] { at },
            at => new { IsNull = at == null, Utc = at.HasValue ? at.Value.UtcDateTime : DateTime.MinValue });

        Connection.QuerySingle<(bool, DateTime)>("SELECT * FROM ext_instant_nullable(NULL::TIMESTAMPTZ)")
            .Should().Be((true, DateTime.MinValue));
        Connection.QuerySingle<(bool, DateTime)>("SELECT * FROM ext_instant_nullable(TIMESTAMPTZ '2024-02-29 13:45:30+02')")
            .Should().Be((false, new DateTime(2024, 2, 29, 11, 45, 30)));
    }
}

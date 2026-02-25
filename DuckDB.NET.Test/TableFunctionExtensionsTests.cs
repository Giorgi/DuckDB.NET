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
    public void RegisterTableFunctionSimplifiedOneParam()
    {
        Connection.RegisterTableFunction("ext_one",
            (int count) => GetEmployees(count),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_one(3);").ToList();

        data.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2"), (3, "Employee3")]);
    }

    [Fact]
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
    public void RegisterTableFunctionSimplifiedComputedColumns()
    {
        Connection.RegisterTableFunction("ext_computed",
            (int count) => GetEmployees(count),
            e => new { FullName = "Dr. " + e.Name, DoubleSalary = e.Salary * 2 });

        var data = Connection.Query<(string, double)>("SELECT * FROM ext_computed(2);").ToList();

        data.Should().BeEquivalentTo([("Dr. Employee1", 100200.0), ("Dr. Employee2", 100400.0)]);
    }

    [Fact]
    public void RegisterTableFunctionSimplifiedMemberInit()
    {
        Connection.RegisterTableFunction("ext_init",
            (int count) => GetEmployees(count),
            e => new EmployeeDto { Id = e.Id, Name = e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_init(2);").ToList();

        data.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);
    }

    [Fact]
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
    public void RegisterTableFunctionSimplifiedThreeParams()
    {
        Connection.RegisterTableFunction("ext_three",
            (int start, int count, string prefix) => Enumerable.Range(start, count).Select(i => new Employee(i, $"{prefix}{i}", i * 1000)),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>("SELECT * FROM ext_three(5, 3, 'Emp');").ToList();

        data.Should().BeEquivalentTo([(5, "Emp5", 5000.0), (6, "Emp6", 6000.0), (7, "Emp7", 7000.0)]);
    }

    [Fact]
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
    public void RegisterTableFunctionSimplifiedNullIntParameter()
    {
        Connection.RegisterTableFunction("ext_null_int",
            (int value) => new[] { new Employee(value, "X", 1) },
            e => new { e.Id });

        Connection.Invoking(con => con.Query<int>("SELECT * FROM ext_null_int(NULL::INTEGER);"))
                  .Should().Throw<DuckDBException>().WithMessage("*Table function 'ext_null_int' argument 1 is NULL, but parameter type 'Int32' is non-nullable.*");
    }

    [Fact]
    public void RegisterTableFunctionSimplifiedNullStringParameter()
    {
        Connection.RegisterTableFunction("ext_null_string",
            (string value) => new[] { new Employee(1, value ?? "default", 1) },
            e => new { e.Name });

        var data = Connection.Query<string>("SELECT * FROM ext_null_string(NULL::VARCHAR);").Single();
        data.Should().Be("default");
    }

    [Fact]
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
    public void RegisterTableFunctionNamedParameter()
    {
        Connection.RegisterTableFunction("ext_named", (int count, [Named] string? prefix) =>
                GetEmployees(count).Select(e => e with { Name = (prefix ?? "") + e.Name }),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_named(2, prefix := 'Dr. ');").ToList();
        data.Should().BeEquivalentTo([(1, "Dr. Employee1"), (2, "Dr. Employee2")]);

        var data2 = Connection.Query<(int, string)>("SELECT * FROM ext_named(2);").ToList();
        data2.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);

        // Explicit NULL should behave the same as omitted
        var data3 = Connection.Query<(int, string)>("SELECT * FROM ext_named(2, prefix := NULL);").ToList();
        data3.Should().BeEquivalentTo([(1, "Employee1"), (2, "Employee2")]);
    }

    [Fact]
    public void RegisterTableFunctionMultipleNamedParameters()
    {
        Connection.RegisterTableFunction("ext_multi_named",
            (int count, [Named] string? prefix, [Named] double? multiplier) =>
                GetEmployees(count).Select(e => e with
                {
                    Name = (prefix ?? "") + e.Name,
                    Salary = e.Salary * (multiplier ?? 1)
                }),
            e => new { e.Id, e.Name, e.Salary });

        var data = Connection.Query<(int, string, double)>(
            "SELECT * FROM ext_multi_named(2, prefix := 'X', multiplier := 2.0);").ToList();
        data.Should().BeEquivalentTo([(1, "XEmployee1", 100200.0), (2, "XEmployee2", 100400.0)]);

        // Provide prefix but omit multiplier
        var data2 = Connection.Query<(int, string, double)>(
            "SELECT * FROM ext_multi_named(2, prefix := 'Y');").ToList();
        data2.Should().BeEquivalentTo([(1, "YEmployee1", 50100.0), (2, "YEmployee2", 50200.0)]);
    }

    [Fact]
    public void RegisterTableFunctionNamedParameterCustomName()
    {
        Connection.RegisterTableFunction("ext_custom_name",
            (int count, [Named("max_rows")] int? limit) =>
                GetEmployees(limit ?? count),
            e => new { e.Id, e.Name });

        var data = Connection.Query<(int, string)>("SELECT * FROM ext_custom_name(10, max_rows := 2);").ToList();
        data.Should().HaveCount(2);
    }

    [Fact]
    public void RegisterTableFunctionNamedParameterNonNullableThrows()
    {
        Connection.RegisterTableFunction("ext_named_nonnull",
            (int count, [Named] int limit) => GetEmployees(count).Take(limit),
            e => new { e.Id, e.Name });

        Connection.Invoking(c => c.Query<(int, string)>("SELECT * FROM ext_named_nonnull(5);"))
            .Should().Throw<DuckDBException>()
            .WithMessage("*named parameter 'limit' is NULL*non-nullable*");
    }
}

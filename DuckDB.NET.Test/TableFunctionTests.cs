using System.Globalization;
using System.Text.Json;
using System.Threading;

namespace DuckDB.NET.Test;

public class TableFunctionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void RegisterTableFunctionWithNoParameters()
    {
        Connection.RegisterTableFunction("answer", () =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("answer", typeof(int)),
            }, new[] { 42 });
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM answer();");
        data.Should().BeEquivalentTo([42]);
    }

    [Fact]
    public void RegisterTableFunctionWithOneParameter()
    {
        Connection.RegisterTableFunction<int>("demo", parameters =>
        {
            var value = parameters[0].GetValue<int>();

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(int)),
            }, Enumerable.Range(0, value));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM demo(30);");
        data.Should().BeEquivalentTo(Enumerable.Range(0, 30));
    }

    [Fact]
    public void RegisterTableFunctionWithTwoParameterTwoColumns()
    {
        var count = 50;

        Connection.RegisterTableFunction<short, string>("demo2", parameters =>
        {
            var start = parameters[0].GetValue<short>();
            var prefix = parameters[1].GetValue<string>();

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(int)),
                new("bar", typeof(string)),
            }, Enumerable.Range(start, count).Select(index => KeyValuePair.Create(index, prefix + index)));
        }, (item, writers, rowIndex) =>
        {
            var pair = (KeyValuePair<int, string>)item;
            writers[0].WriteValue(pair.Key, rowIndex);
            writers[1].WriteValue(pair.Value, rowIndex);
        });

        var data = Connection.Query<(int, string)>("SELECT * FROM demo2(30::SmallInt, 'DuckDB');").ToList();

        data.Select(tuple => tuple.Item1).Should().BeEquivalentTo(Enumerable.Range(30, count));
        data.Select(tuple => tuple.Item2).Should().BeEquivalentTo(Enumerable.Range(30, count).Select(i => $"DuckDB{i}"));
    }

    [Fact]
    public void RegisterTableFunctionWithThreeParameters()
    {
        var count = 30;
        var startDate = new DateTime(2024, 11, 6);
        var minutesParam = 10;
        var secondsParam = 2.5;

        Connection.RegisterTableFunction<DateTime, long, double>("demo3", parameters =>
        {
            var date = parameters[0].GetValue<DateTime>();
            var minutes = parameters[1].GetValue<long>();
            var seconds = parameters[2].GetValue<double>();

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(DateTime)),
            }, Enumerable.Range(0, count).Select(i => date.AddDays(i).AddMinutes(minutes).AddSeconds(seconds)));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((DateTime)item, rowIndex);
        });

        var data = Connection.Query<DateTime>("SELECT * FROM demo3('2024-11-06'::TIMESTAMP, 10, 2.5 );").ToList();

        var dateTimes = Enumerable.Range(0, count).Select(i => startDate.AddDays(i).AddMinutes(minutesParam).AddSeconds(secondsParam));
        data.Should().BeEquivalentTo(dateTimes);
    }

    [Fact]
    public void RegisterTableFunctionWithFourParameters()
    {
        var guid = Guid.NewGuid();

        Connection.RegisterTableFunction<bool, decimal, byte, Guid>("demo4", parameters =>
        {
            var param1 = parameters[0].GetValue<bool>();
            var param2 = parameters[1].GetValue<decimal>();
            var param3 = parameters[2].GetValue<byte>();
            var param4 = parameters[3].GetValue<Guid>();

            var enumerable = param4.ToByteArray(param1).Append(param3);

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(byte)),
            }, enumerable);
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((byte)item, rowIndex);
        });

        var data = Connection.Query<byte>($"SELECT * FROM demo4(false, 10::DECIMAL(18, 3), 4::UTINYINT, '{guid}'::UUID );").ToList();

        var bytes = guid.ToByteArray(false).Append((byte)4);
        data.Should().BeEquivalentTo(bytes);
    }

    [Fact]
    public void RegisterTableFunctionWithDecimalCultureInvariantParameters()
    {
        var currentCulture = Thread.CurrentThread.CurrentCulture;

        try
        {
            // Set the current culture to Georgian (ka-GE), which uses a comma as the decimal point separator
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("ka-GE");

            // Verify that the decimal separator for numbers in this culture is indeed a comma
            Thread.CurrentThread.CurrentCulture.NumberFormat.NumberDecimalSeparator.Should().Be(",");

            Connection.RegisterTableFunction<decimal>("demo_decimal", parameters =>
            {
                var param1 = parameters[0].GetValue<decimal>();

                return new TableFunction(new List<ColumnInfo>
                {
                    new("foo", typeof(decimal)),
                }, new[] { param1 });
            }, (item, writers, rowIndex) =>
            {
                writers[0].WriteValue((decimal)item, rowIndex);
            });

            var data = Connection.Query<decimal>($"SELECT * FROM demo_decimal(10.2::DECIMAL(18, 3));").ToList();

            data.Should().BeEquivalentTo([10.2m]);
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = currentCulture;
        }
    }

    [Fact]
    public void RegisterTableFunctionWithEmptyResult()
    {
        Connection.RegisterTableFunction<sbyte, ushort, uint, ulong, float>("demo5", parameters =>
        {
            var param1 = parameters[0].GetValue<sbyte>();
            var param2 = parameters[1].GetValue<ushort>();
            var param3 = parameters[2].GetValue<uint>();
            var param4 = parameters[3].GetValue<ulong>();
            var param5 = parameters[4].GetValue<float>();

            param1.Should().Be(1);
            param2.Should().Be(2);
            param3.Should().Be(3);
            param4.Should().Be(4);
            param5.Should().Be(5.6f);

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(int)),
            }, Enumerable.Empty<int>());
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM demo5(1::TINYINT, 2::USMALLINT, 3::UINTEGER, 4::UBIGINT, 5.6);").ToList();

        data.Should().BeEquivalentTo(Enumerable.Empty<int>());
    }

    [Fact]
    public void RegisterTableFunctionWithBigInteger()
    {
        Connection.RegisterTableFunction<BigInteger, TimeSpan>("demo6", parameters =>
        {
            var param1 = parameters[0].GetValue<BigInteger>();
            var param2 = parameters[1].GetValue<TimeSpan>();

            var timeSpans = param1.ToByteArray().Select(b => param2.Add(TimeSpan.FromDays(b)));

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(TimeSpan)),
            }, timeSpans);
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((TimeSpan)item, rowIndex);
        });

        var data = Connection.Query<TimeSpan>("SELECT * FROM demo6('123456789876543210'::HUGEINT, '24:00:00'::INTERVAL);").ToList();

        data.Should().BeEquivalentTo(BigInteger.Parse("123456789876543210").ToByteArray().Select(b => TimeSpan.FromDays(1 + b)));
    }

    [Fact]
    public void RegisterTableFunctionWithErrors()
    {
        Connection.RegisterTableFunction<string>("bind_err", _ => throw new Exception("bind_err_msg"), (_, _, _) =>
        {
        });

        Connection.Invoking(con => con.Query<int>("SELECT * FROM bind_err('')")).Should().Throw<DuckDBException>().WithMessage("*bind_err_msg*");

        Connection.RegisterTableFunction<string>("map_err", _ =>
        {
            return new TableFunction([new ColumnInfo("t1", typeof(string))], new[] { "a" });
        }, (_, _, _) => throw new NotSupportedException("map_err_msg"));

        Connection.Invoking(con => con.Query<int>("SELECT * FROM map_err('')")).Should().Throw<DuckDBException>().WithMessage("*map_err_msg*");
    }

    [Fact]
    public void RegisterTableFunctionWithNullParameter()
    {
        Connection.RegisterTableFunction<int>("nullParam", parameters =>
        {
            parameters[0].IsNull().Should().BeTrue();

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(int)),
            }, Enumerable.Empty<int>());
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM nullParam(NULL::INTEGER);").ToList();

        data.Should().BeEquivalentTo(Enumerable.Empty<int>());
    }

    [Fact]
    public void RegisterTableFunctionWithNullableParameterType()
    {
        Connection.RegisterTableFunction<int?>("nullableParam", parameters =>
        {
            parameters[0].IsNull().Should().BeTrue();

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(int)),
            }, Enumerable.Empty<int>());
        },
        (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM nullableParam(NULL::INTEGER);").ToList();

        data.Should().BeEquivalentTo(Enumerable.Empty<int>());
    }

    [Fact]
    public void RegisterFunctionWithDateOnlyTimeOnlyParameters()
    {
        Connection.RegisterTableFunction<DateOnly, TimeOnly>("demo7", parameters =>
        {
            var date = parameters[0].GetValue<DateOnly>();
            var time = parameters[1].GetValue<TimeOnly>();

            var dateTime = date.ToDateTime(time);

            return new TableFunction(new List<ColumnInfo>
            {
                new("foo", typeof(DateTime)),
            }, new[] { dateTime });
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((DateTime)item, rowIndex);
        });

        var data = Connection.Query<DateTime>("SELECT * FROM demo7('2024-11-06'::DATE, '10:30:24'::TIME);").ToList();

        data.Should().BeEquivalentTo([new DateTime(2024, 11, 6, 10, 30, 24)]);
    }

    [Fact]
    public void RegisterTableFunctionWithExplicitTimestampTypes()
    {
        Connection.RegisterTableFunction("demo_explicit_timestamps",
            parameters =>
            {
                // All timestamp variants should be retrievable as DateTime
                var timestampS = parameters[0].GetValue<DateTime>();
                var timestampMs = parameters[1].GetValue<DateTime>();
                var timestampNs = parameters[2].GetValue<DateTime>();
                var timestampTz = parameters[3].GetValue<DateTime>();

                // TimestampS: second precision - fractional seconds should be 0
                timestampS.Should().Be(new DateTime(2024, 11, 6, 15, 30, 45));

                // TimestampMs: millisecond precision
                timestampMs.Should().Be(new DateTime(2024, 11, 6, 15, 30, 45, 123));

                // TimestampNs: nanosecond precision is preserved up to tick precision (100ns)
                // '2024-11-06 15:30:45.123456789' -> 123 ms + 456 μs + 789 ns
                // 789 ns / 100 = 7 additional ticks (since 1 tick = 100ns)
                timestampNs.Should().Be(new DateTime(2024, 11, 6, 15, 30, 45, 123).AddTicks(4567));

                // TimestampTz: timezone timestamp gets converted to UTC
                // The exact value depends on the session timezone setting
                timestampTz.Year.Should().Be(2024);
                timestampTz.Month.Should().Be(11);
                timestampTz.Day.Should().Be(6);

                return new TableFunction(new List<ColumnInfo>
                {
                    new("result", typeof(string)),
                }, new[] { "success" });
            },
            (item, writers, rowIndex) =>
            {
                writers[0].WriteValue((string)item, rowIndex);
            },
            DuckDBType.TimestampS,
            DuckDBType.TimestampMs,
            DuckDBType.TimestampNs,
            DuckDBType.TimestampTz);

        var data = Connection.Query<string>(
            "SELECT * FROM demo_explicit_timestamps(" +
            "'2024-11-06 15:30:45'::TIMESTAMP_S, " +
            "'2024-11-06 15:30:45.123'::TIMESTAMP_MS, " +
            "'2024-11-06 15:30:45.123456789'::TIMESTAMP_NS, " +
            "'2024-11-06 15:30:45.123456'::TIMESTAMPTZ);").ToList();

        data.Should().BeEquivalentTo("success");
    }

    [Fact]
    public void RegisterTableFunctionWithTimeTzParameter()
    {
        Connection.RegisterTableFunction("demo_timetz",
            parameters =>
            {
                var timeTz = parameters[0].GetValue<DateTimeOffset>();

                // TimeTz stores time with timezone offset
                // The time should be 10:30:45 with +02:00 offset
                timeTz.Hour.Should().Be(10);
                timeTz.Minute.Should().Be(30);
                timeTz.Second.Should().Be(45);
                timeTz.Offset.Should().Be(TimeSpan.FromHours(2));

                return new TableFunction(new List<ColumnInfo>
                {
                    new("result", typeof(string)),
                }, new[] { "success" });
            },
            (item, writers, rowIndex) =>
            {
                writers[0].WriteValue((string)item, rowIndex);
            },
            DuckDBType.TimeTz);

        var data = Connection.Query<string>("SELECT * FROM demo_timetz('10:30:45+02:00'::TIMETZ);").ToList();

        data.Should().BeEquivalentTo("success");
    }

    [Fact]
    public void TableFunctionBindError_PreservesInnerException()
    {
        var originalException = new InvalidOperationException("custom bind error");

        Connection.RegisterTableFunction<string>("bind_inner_err", _ => throw originalException, (_, _, _) => { });

        var act = () => Connection.Query<int>("SELECT * FROM bind_inner_err('')");
        var ex = act.Should().Throw<DuckDBException>().Which;

        ex.Message.Should().Contain("custom bind error");
        ex.InnerException.Should().BeOfType<InvalidOperationException>();
        ex.InnerException!.Message.Should().Be("custom bind error");
        ex.InnerException.Should().BeSameAs(originalException);
    }

    [Fact]
    public void TableFunctionMapError_PreservesInnerException()
    {
        var originalException = new NotSupportedException("custom map error");

        Connection.RegisterTableFunction<string>("map_inner_err", _ =>
        {
            return new TableFunction([new ColumnInfo("col1", typeof(string))], new[] { "a" });
        }, (_, _, _) => throw originalException);

        var act = () => Connection.Query<int>("SELECT * FROM map_inner_err('')");
        var ex = act.Should().Throw<DuckDBException>().Which;

        ex.Message.Should().Contain("custom map error");
        ex.InnerException.Should().BeOfType<NotSupportedException>();
        ex.InnerException!.Message.Should().Be("custom map error");
        ex.InnerException.Should().BeSameAs(originalException);
    }

    [Theory]
    [InlineData(100, false, "card_estimated")]
    [InlineData(50, true, "card_exact")]
    public void RegisterTableFunctionWithCardinality(int count, bool isExact, string funcName)
    {
        var expectedData = Enumerable.Range(0, count).ToList();

        Connection.RegisterTableFunction(funcName, parameters =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("value", typeof(int)),
            }, expectedData, cardinality: new CardinalityHint((ulong)count, IsExact: isExact));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>($"SELECT * FROM {funcName}();").ToList();
        data.Should().BeEquivalentTo(expectedData);
    }

    [Theory]
    [InlineData(42, false, "plan_estimated")]
    [InlineData(50, true, "plan_exact")]
    public void RegisterTableFunctionWithCardinality_AppearsInQueryPlan(int cardinality, bool isExact, string funcName)
    {
        Connection.RegisterTableFunction(funcName, _ =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("value", typeof(int)),
            }, Enumerable.Range(0, 50), cardinality: new CardinalityHint((ulong)cardinality, IsExact: isExact));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var plan = Connection.Query<(string, string)>($"EXPLAIN (FORMAT JSON) SELECT * FROM {funcName}();").First();
        var json = JsonDocument.Parse(plan.Item2);
        var reported = json.RootElement[0]
            .GetProperty("extra_info")
            .GetProperty("Estimated Cardinality")
            .GetString();

        reported.Should().Be(cardinality.ToString());
    }

    [Fact]
    public void RegisterTableFunctionWithListColumn()
    {
        var sourceData = new List<List<int>>
        {
            new() { 1, 2, 3 },
            new() { 4, 5 },
            new() { },
            null,
        };

        Connection.RegisterTableFunction("list_func", () =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("numbers", typeof(List<int>)),
            }, sourceData);
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((List<int>)item, rowIndex);
        });

        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT numbers FROM list_func();";
        using var reader = command.ExecuteReader();

        reader.Read();
        reader.GetFieldValue<List<int>>(0).Should().BeEquivalentTo(new List<int> { 1, 2, 3 });

        reader.Read();
        reader.GetFieldValue<List<int>>(0).Should().BeEquivalentTo(new List<int> { 4, 5 });

        reader.Read();
        reader.GetFieldValue<List<int>>(0).Should().BeEquivalentTo(new List<int>());

        reader.Read();
        reader.IsDBNull(0).Should().BeTrue();

        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void RegisterTableFunctionWithListColumn_ListHasAny()
    {
        var sourceData = new List<List<int>>
        {
            new() { 1, 2, 3 },
            new() { 4, 5 },
            new() { 6, 7, 8, 9 },
            new() { },
        };

        Connection.RegisterTableFunction("list_func_has_any", () =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("numbers", typeof(List<int>)),
            }, sourceData);
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((List<int>)item, rowIndex);
        });

        var data = Connection.Query<bool>("SELECT list_has_any(numbers, [3]) FROM list_func_has_any();").ToList();
        data.Should().BeEquivalentTo([true, false, false, false]);
    }

    [Fact]
    public void RegisterTableFunctionWithNestedListColumn()
    {
        var sourceData = new List<List<List<int?>>>
        {
            new() { new() { 1, 2 }, new() { 3, null, 5 } },
            new() { new() { 10 }, null, new() { 20, 30 }, new() { 40 } },
        };

        Connection.RegisterTableFunction("list_func_nested", () =>
        {
            return new TableFunction(new List<ColumnInfo>
            {
                new("nested", typeof(List<List<int?>>)),
            }, sourceData);
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((List<List<int?>>)item, rowIndex);
        });

        // flatten reduces one level of nesting
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT flatten(nested) FROM list_func_nested();";
        using var reader = command.ExecuteReader();

        reader.Read();
        reader.GetFieldValue<List<int?>>(0).Should().BeEquivalentTo([1, 2, 3, (int?)null, 5],
            options => options.WithStrictOrdering());

        reader.Read();
        reader.GetFieldValue<List<int?>>(0).Should().BeEquivalentTo([10, 20, 30, 40],
            options => options.WithStrictOrdering());

        reader.Read().Should().BeFalse();
    }

    private record Row(int Id, string Name, decimal Salary);

    private static readonly List<Row> ProjectionRows =
    [
        new(1, "alice",   1000m),
        new(2, "bob",     2000m),
        new(3, "carol",   3000m),
    ];

    [Fact]
    public void ProjectionPushdown_LegacyData_PrunedColumn()
    {
        Connection.RegisterTableFunction("pp_legacy_prune", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, ProjectionRows),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        var names = Connection.Query<string>("SELECT name FROM pp_legacy_prune();").ToList();
        names.Should().BeEquivalentTo(["alice", "bob", "carol"], o => o.WithStrictOrdering());
    }

    [Fact]
    public void ProjectionPushdown_LegacyData_Reordered()
    {
        Connection.RegisterTableFunction("pp_legacy_reorder", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, ProjectionRows),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT salary, id FROM pp_legacy_reorder() ORDER BY id;";
        using var reader = command.ExecuteReader();

        reader.FieldCount.Should().Be(2);
        reader.GetName(0).Should().Be("salary");
        reader.GetName(1).Should().Be("id");

        var results = new List<(decimal salary, int id)>();
        while (reader.Read())
        {
            results.Add((reader.GetDecimal(0), reader.GetInt32(1)));
        }

        results.Should().BeEquivalentTo([
            (1000m, 1), (2000m, 2), (3000m, 3)
        ], o => o.WithStrictOrdering());
    }

    [Fact]
    public void ProjectionPushdown_LegacyData_CountStar()
    {
        Connection.RegisterTableFunction("pp_legacy_count", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
            }, ProjectionRows),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
        });

        var count = Connection.Query<int>("SELECT COUNT(*) FROM pp_legacy_count();").Single();
        count.Should().Be(3);
    }

    [Fact]
    public void ProjectionPushdown_Factory_Identity()
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction("pp_factory_identity", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        var rows = Connection.Query<(int, string, decimal)>("SELECT * FROM pp_factory_identity() ORDER BY id;").ToList();
        rows.Should().HaveCount(3);
        rows[0].Should().Be((1, "alice", 1000m));

        captured.Should().NotBeNull();
        captured.Select(p => (p.Index, p.Name)).Should()
            .BeEquivalentTo([(0, "id"), (1, "name"), (2, "salary")], o => o.WithStrictOrdering());
    }

    [Fact]
    public void ProjectionPushdown_Factory_Pruned()
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction("pp_factory_prune", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        var names = Connection.Query<string>("SELECT name FROM pp_factory_prune();").ToList();
        names.Should().BeEquivalentTo(["alice", "bob", "carol"], o => o.WithStrictOrdering());

        captured.Should().NotBeNull();
        captured.Should().HaveCount(1);
        captured[0].Index.Should().Be(1);
        captured[0].Name.Should().Be("name");
        captured[0].Type.Should().Be(typeof(string));
    }

    [Fact]
    public void ProjectionPushdown_Factory_Reordered()
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction("pp_factory_reorder", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT salary, id FROM pp_factory_reorder() ORDER BY id;";
        using var reader = command.ExecuteReader();

        while (reader.Read()) { }

        captured.Should().NotBeNull();
        captured.Select(p => p.Index).Should().BeEquivalentTo([2, 0], o => o.WithStrictOrdering());
    }

    [Fact]
    public void ProjectionPushdown_Factory_CountStar()
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction("pp_factory_count", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
        });

        var count = Connection.Query<int>("SELECT COUNT(*) FROM pp_factory_count();").Single();
        count.Should().Be(3);

        // DuckDB typically projects a single minimal column for COUNT(*); assert the factory saw a pruned subset.
        captured.Should().NotBeNull();
        captured.Count.Should().Be(1);
    }

    [Fact]
    public void ProjectionPushdown_Factory_ThrowsInInit_PreservesInnerException()
    {
        var originalException = new InvalidOperationException("custom factory error");

        Connection.RegisterTableFunction("pp_factory_throw", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
            }, _ => throw originalException),
        (_, _, _) => { });

        var act = () => Connection.Query<int>("SELECT id FROM pp_factory_throw();").ToList();
        var ex = act.Should().Throw<DuckDBException>().Which;

        ex.Message.Should().Contain("custom factory error");
        ex.InnerException.Should().BeSameAs(originalException);
    }

    [Fact]
    public void ProjectionPushdown_Probe_DuplicateSelection()
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction("pp_probe_dup", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT name, name, name FROM pp_probe_dup();";
        using var reader = command.ExecuteReader();

        reader.FieldCount.Should().Be(3);
        var firstRow = new List<string>();
        reader.Read();
        firstRow.Add(reader.GetString(0));
        firstRow.Add(reader.GetString(1));
        firstRow.Add(reader.GetString(2));
        firstRow.Should().AllBe("alice");

        // DuckDB deduplicates the projection — the scan produces `name` once and
        // a projection operator above the scan replicates it for each duplicate reference.
        captured.Should().NotBeNull();
        captured.Count.Should().Be(1);
        captured.Select(p => p.Name).Should().Equal("name");
    }

    [Theory]
    [InlineData("pp_probe_a", "SELECT * FROM pp_probe_a();",                        new[] { 0, 1, 2 })]
    [InlineData("pp_probe_b", "SELECT id, name, salary FROM pp_probe_b();",         new[] { 0, 1, 2 })]
    [InlineData("pp_probe_c", "SELECT salary, id, name FROM pp_probe_c();",         new[] { 2, 0, 1 })]
    [InlineData("pp_probe_d", "SELECT name, id FROM pp_probe_d();",                 new[] { 1, 0 })]
    [InlineData("pp_probe_e", "SELECT salary FROM pp_probe_e();",                   new[] { 2 })]
    public void ProjectionPushdown_Probe_IndicesReflectSelectOrder(string funcName, string query, int[] expectedIndices)
    {
        IReadOnlyList<ProjectedColumn> captured = null;

        Connection.RegisterTableFunction(funcName, () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("name", typeof(string)),
                new("salary", typeof(decimal)),
            }, projected =>
            {
                captured = projected;
                return ProjectionRows;
            }),
        (item, writers, rowIndex) =>
        {
            var row = (Row)item!;
            writers[0].WriteValue(row.Id, rowIndex);
            writers[1].WriteValue(row.Name, rowIndex);
            writers[2].WriteValue(row.Salary, rowIndex);
        });

        using var command = Connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        while (reader.Read()) { }

        captured.Should().NotBeNull();
        captured.Select(p => p.Index).Should().Equal(expectedIndices);
    }

    [Fact]
    public void ProjectionPushdown_LegacyData_ListColumnPruned()
    {
        var callCount = 0;
        var sourceData = new List<(int id, List<int> numbers)>
        {
            (1, [1, 2, 3]),
            (2, [4, 5]),
            (3, [6, 7, 8, 9]),
        };

        Connection.RegisterTableFunction("pp_list_prune", () =>
            new TableFunction(new List<ColumnInfo>
            {
                new("id", typeof(int)),
                new("numbers", typeof(List<int>)),
            }, sourceData),
        (item, writers, rowIndex) =>
        {
            callCount++;
            var row = ((int id, List<int> numbers))item!;
            writers[0].WriteValue(row.id, rowIndex);
            writers[1].WriteValue(row.numbers, rowIndex);
        });

        var ids = Connection.Query<int>("SELECT id FROM pp_list_prune() ORDER BY id;").ToList();
        ids.Should().BeEquivalentTo([1, 2, 3], o => o.WithStrictOrdering());
        callCount.Should().Be(3);
    }
}

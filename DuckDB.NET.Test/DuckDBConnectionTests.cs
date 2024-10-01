using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using System;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBConnectionTests
{
    [Fact]
    [Trait("Category", "Long Running")]
    public async Task ConnectionSpeed()
    {
        const int taskCount = 5;
        const int fileCount = taskCount * 5;
        const int operationCount = 300;
        const int totalOperations = taskCount * operationCount;

        using var files = new DisposableFileList(fileCount, "db");

        //open and close files with some overlap
        var openAndClose = new Func<int, Task>(async ti =>
        {
            var rnd = new Random(ti);

            for (int i = 0; i < operationCount; i++)
            {
                var cs = files[rnd.Next(fileCount)].ConnectionString;
                await using var duckDBConnection = new DuckDBConnection(cs);
                await duckDBConnection.OpenAsync();
            }
        });

        var tasks = new Task[taskCount];

        var stopwatch = Stopwatch.StartNew();

        //it's hammer time baby!
        for (int i = 0; i < taskCount; i++)
        {
            var index = i;
            tasks[i] = Task.Run(async () => await openAndClose(index));
        }

        await Task.WhenAll(tasks);

        var elapsed = stopwatch.Elapsed.TotalSeconds;

        var operationsPerSec = totalOperations / elapsed;

        Console.WriteLine($"Operations Per Second:{operationsPerSec:0.0}");

        //dispose here to make sure there isn't a connection still attached
        files.Dispose();
    }

    [Fact]
    [Trait("Category", "Baseline")]
    public async Task ExceptionOnDoubleClose()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
        await duckDBConnection.OpenAsync();

        await duckDBConnection.CloseAsync();
        await duckDBConnection.Invoking(async connection => await connection.CloseAsync())
            .Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    [Trait("Category", "Baseline")]
    public async Task ExceptionOnDisposeThenClose()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
        await duckDBConnection.OpenAsync();

        await duckDBConnection.DisposeAsync();
        await duckDBConnection.Invoking(async connection => await connection.CloseAsync())
            .Should().ThrowAsync<InvalidOperationException>();
    }

    /// <summary>
    /// Spin up set of tasks to randomly create connections, insert data, and close connections.
    /// </summary>
    /// <returns></returns>
    [Fact]
    [Trait("Category", "Long Running")]
    public async Task MultiThreadedStress()
    {
        //with 1 task per file, should be good mix of reusing connections
        //and disposing of them
        const int fileCount = 20;
        const int taskCount = 20;
        const int insertionCount = 1000;
        const int totalInsertions = taskCount * insertionCount;

        var files = new DisposableFileList(fileCount, "db");

        foreach (var f in files)
        {
            await using var duckDBConnection = new DuckDBConnection(f.ConnectionString);
            await duckDBConnection.OpenAsync();

            var createTable = "CREATE TABLE INSERTIONS(TASK_ID INTEGER, INSERTION_INDEX INTEGER);";
            await duckDBConnection.ExecuteAsync(createTable);
        }

        var insertionsWithRandomDelay = new Func<int, Task>(async ti =>
        {
            var rnd = new Random(ti);

            for (int i = 0; i < insertionCount; i++)
            {
                //pick a random connection string for each test and jitter delays

                await Task.Delay(TimeSpan.FromMilliseconds(rnd.Next(0, 10)));

                var cs = files[rnd.Next(files.Count)].ConnectionString;
                await using var duckDBConnection = new DuckDBConnection(cs);
                await duckDBConnection.OpenAsync();

                var insertAValue = $"INSERT INTO INSERTIONS VALUES ({ti}, {i});";
                await duckDBConnection.ExecuteAsync(insertAValue);
            }
        });

        var tasks = new Task[taskCount];

        //it's hammer time baby!
        for (int i = 0; i < taskCount; i++)
        {
            var index = i;
            tasks[i] = Task.Run(async () => await insertionsWithRandomDelay(index));
        }

        await Task.WhenAll(tasks);

        //sanity check of insertions
        int insertionCountPostRun = 0;

        foreach (var f in files)
        {
            var cs = f.ConnectionString;
            await using var duckDBConnection = new DuckDBConnection(cs);
            await duckDBConnection.OpenAsync();

            var insertions = await duckDBConnection.QuerySingleAsync<int>("SELECT COUNT(*) FROM INSERTIONS;");
            insertions.Should().BeGreaterThan(0);
            insertionCountPostRun += insertions;

            Console.WriteLine($"{insertions:0} Insertions for {cs}");
        }

        insertionCountPostRun.Should().Be(totalInsertions, $"Insertions don't add up?");

        //dispose here to make sure there isn't a connection still attached
        files.Dispose();
    }

    [Fact]
    [Trait("Category", "Baseline")]
    public async Task NoExceptionOnDoubleDispose()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
        await duckDBConnection.OpenAsync();

        await duckDBConnection.Invoking(async connection =>
        {
            await connection.DisposeAsync();
            await connection.DisposeAsync();
        }).Should().NotThrowAsync();
    }

    [Fact]
    [Trait("Category", "Baseline")]
    public async Task NoExceptionCloseThenDispose()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
        await duckDBConnection.OpenAsync();

        await duckDBConnection.Invoking(async connection =>
        {
            await connection.CloseAsync();
            await connection.DisposeAsync();
        }).Should().NotThrowAsync();
    }

    [Fact]
    [Trait("Category", "Baseline")]
    public async Task SingleThreadedOpenAndCloseOfSameFile()
    {
        using var db1 = DisposableFile.GenerateInTemp("db", 1);
        var cs = db1.ConnectionString;

        await using var duckDBConnection = new DuckDBConnection(cs);
        await duckDBConnection.OpenAsync();

        var createTable = "CREATE TABLE INSERTIONS(TASK_ID INTEGER, INSERTION_INDEX INTEGER);";
        await duckDBConnection.ExecuteAsync(createTable);

        await using var dd1 = new DuckDBConnection(cs);
        await using var dd2 = new DuckDBConnection(cs);

        const int reps = 10;

        for (int i = 0; i < reps; i++)
        {
            Console.WriteLine(i);

            await dd1.OpenAsync();

            var insertAValue = $"INSERT INTO INSERTIONS VALUES ({1}, {i});";
            await dd1.ExecuteAsync(insertAValue);

            await dd2.OpenAsync();

            insertAValue = $"INSERT INTO INSERTIONS VALUES ({2}, {i});";
            await dd2.ExecuteAsync(insertAValue);

            await dd1.CloseAsync();
            await dd2.CloseAsync();
        }

        var expectedInsertions = 2 * reps;
        var insertions = await duckDBConnection.QuerySingleAsync<int>("SELECT COUNT(*) FROM INSERTIONS;");
        insertions.Should().Be(expectedInsertions);
    }

    [Fact]
    public void QueryAgainstClosedConnection()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.State.Should().Be(ConnectionState.Closed);

        connection.Invoking(con =>
        {
            var command = con.CreateCommand();
            command.CommandText = "SELECT 42;";
            command.ExecuteScalar();
        }).Should().ThrowExactly<InvalidOperationException>();

        connection.Open();
        connection.State.Should().Be(ConnectionState.Open);

        var command = connection.CreateCommand();
        command.CommandText = "SELECT 42;";
        command.ExecuteScalar().Should().Be(42);
    }

    [Fact]
    public void OpenConnectionTwiceError()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");

        connection.Open();
        connection.Invoking(connection => connection.Open()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void MultipleInMemoryConnectionsSeparateDatabases()
    {
        var tableCount = 0;

        using var firstConnection = new DuckDBConnection("DataSource=:memory:");
        using var secondConnection = new DuckDBConnection("DataSource=:memory:");

        firstConnection.Open();

        var command = firstConnection.CreateCommand();
        command.CommandText = "CREATE TABLE t1 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        tableCount.Should().Be(1);

        // connection 2
        tableCount = 0;
        secondConnection.Open();
        command = secondConnection.CreateCommand();

        command.CommandText = "CREATE TABLE t2 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        tableCount.Should().Be(1);
    }

    [Fact]
    public void MultipleInMemoryConnectionsSharedDatabases()
    {
        var tableCount = 0;

        using var firstConnection = new DuckDBConnection("DataSource=:memory:?cache=shared");
        using var secondConnection = new DuckDBConnection("DataSource=:memory:?cache=shared");
        using var thirdConnection = new DuckDBConnection("DataSource=:memory:");

        firstConnection.Open();

        var command = firstConnection.CreateCommand();
        command.CommandText = "CREATE TABLE t1 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        tableCount.Should().Be(1);

        // connection 2
        tableCount = 0;
        secondConnection.Open();
        command = secondConnection.CreateCommand();

        command.CommandText = "CREATE TABLE t2 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        tableCount.Should().Be(2);

        var unsharedConnectionTableCount = 0;
        thirdConnection.Open();
        command = thirdConnection.CreateCommand();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                unsharedConnectionTableCount++;
            }
        }

        unsharedConnectionTableCount.Should().Be(0);
    }

    [Fact]
    public void DuplicateNotInMemoryConnectionError()
    {
        using var db1 = DisposableFile.GenerateInTemp("db", 1);
        var cs = db1.ConnectionString;

        using var duckDBConnection = new DuckDBConnection(cs);
        duckDBConnection.Open();

        duckDBConnection.Invoking(connection => connection.Duplicate()).Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void DuplicateInMemoryNotOpenedConnectionError()
    {
        using var duckDBConnection = new DuckDBConnection("DataSource =:memory:");

        duckDBConnection.Invoking(connection => connection.Duplicate()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void DuplicateInMemoryConnection()
    {
        var tableCount = 0;

        using var firstConnection = new DuckDBConnection("DataSource=:memory:");

        firstConnection.Open();

        var command = firstConnection.CreateCommand();
        command.CommandText = "CREATE TABLE t1 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        Assert.Equal(1, tableCount);

        using var secondConnection = firstConnection.Duplicate();
        // connection 2
        tableCount = 0;
        secondConnection.Open();
        command = secondConnection.CreateCommand();

        command.CommandText = "CREATE TABLE t2 (foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "show tables;";
        using (var dataReader = command.ExecuteReader())
        {
            while (dataReader.Read())
            {
                tableCount++;
            }
        }

        Assert.Equal(2, tableCount);
    }

    [Theory]
    [InlineData("threads", 2)]
    [InlineData("Threads", 8)]
    [InlineData("threads", 20)]
    [InlineData("Threads", 100)]
    [InlineData("Threads", 0)]
    [InlineData("threads", -100)]
    public void ConnectionStringSetThreadsOption(string optionName, int threads)
    {
        using var connection = new DuckDBConnection($"DataSource=:memory:;{optionName}={threads}");
        if (threads > 0)
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT current_setting('threads');";
            var value = command.ExecuteScalar();
            value.Should().Be(threads);
        }
        else
        {
            connection.Invoking(c => c.Open()).Should().Throw<DuckDBException>();
        }
    }

    [Theory]
    [InlineData("automatic")]
    [InlineData("AUTOMATIC")]
    [InlineData("READ_WRITE")]
    public void ConnectionStringSetAccessModeOption(string accessMode)
    {
        using var connection = new DuckDBConnection($"DataSource=:memory:;access_mode={accessMode}");
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT current_setting('access_mode');";
            var value = command.ExecuteScalar();
            value.Should().Be(accessMode.ToLower());
        }
    }

    [Theory]
    [InlineData("automatic", 2)]
    [InlineData("AUTOMATIC", 8)]
    [InlineData("READ_WRITE", 100)]
    public void ConnectionStringSetThreadsAndAccessModeOption(string accessMode, int threads)
    {
        using var connection = new DuckDBConnection($"DataSource=:memory:;Access_Mode={accessMode};Threads={threads}");
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT current_setting('access_mode');";
            var value = command.ExecuteScalar();
            value.Should().Be(accessMode.ToLower());

            command.CommandText = "SELECT current_setting('threads');";
            value = command.ExecuteScalar();
            value.Should().Be(threads);
        }
    }

    [Fact]
    public void ConnectionStateHandlerIsCalledOnOpen()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        using var connection = new DuckDBConnection(dbInfo.ConnectionString);
        var handlerCalled = false;
        connection.StateChange += Assert;
        connection.Open();
        connection.StateChange -= Assert; // otherwise the dispose/close will trigger the assert
        
        handlerCalled.Should().BeTrue();
        return;

        void Assert(object sender, StateChangeEventArgs args)
        {
            args.OriginalState.Should().Be(ConnectionState.Closed);
            args.CurrentState.Should().Be(ConnectionState.Open);
            handlerCalled = true;
        }
    }

    [Fact]
    public async Task ConnectionStateHandlerIsCalledOnClose()
    {
        using var dbInfo = DisposableFile.GenerateInTemp("db");
        await using var connection = new DuckDBConnection(dbInfo.ConnectionString);
        var handlerCalled = false;
        await connection.OpenAsync();
        connection.StateChange += Assert; 
        await connection.CloseAsync();
        
        handlerCalled.Should().BeTrue();
        return;

        void Assert(object sender, StateChangeEventArgs args)
        {
            args.OriginalState.Should().Be(ConnectionState.Open);
            args.CurrentState.Should().Be(ConnectionState.Closed);
            handlerCalled = true;
        }
    }
}
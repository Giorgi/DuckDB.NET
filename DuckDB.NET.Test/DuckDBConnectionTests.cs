using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using System;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace DuckDB.NET.Test
{
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
        [Trait("Category","Baseline")]
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

            await duckDBConnection.DisposeAsync();
            await duckDBConnection.DisposeAsync();
        }

        [Fact]
        [Trait("Category", "Baseline")]
        public async Task NoExceptionCloseThenDispose()
        {
            using var dbInfo = DisposableFile.GenerateInTemp("db");
            await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
            await duckDBConnection.OpenAsync();

            await duckDBConnection.CloseAsync();
            await duckDBConnection.DisposeAsync();
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
    }
}
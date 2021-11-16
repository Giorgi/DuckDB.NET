using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace DuckDB.NET.Test
{
    [TestClass]
    public class DuckDBConnectionTests
    {
        [TestMethod]
        [TestCategory("Long Running")]
        public async Task ConnectionSpeed()
        {
            const int taskCount = 5;
            const int fileCount = taskCount * 5;
            const int operationCount = 300;
            const int totalOperations = taskCount * operationCount;

            var files = new DisposableFile[fileCount];

            for (int i = 0; i < fileCount; i++)
            {
                files[i] = TestHelper.GetDisposableFile("db", i);
            }

            var connectionStrings = files
                .Select(f => $"DataSource={f.FileName}")
                .ToArray();

            //open and close files with some overlap
            var openAndClose = new Func<int, Task>(async ti =>
            {
                var rnd = new Random(ti);

                for (int i = 0; i < operationCount; i++)
                {
                    var cs = connectionStrings[rnd.Next(fileCount)];
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
            foreach (var f in files)
            {
                f.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Baseline")]
        [ExpectedException(typeof(InvalidOperationException), "Did not throw expected exception")]
        public async Task ExceptionOnDoubleClose()
        {
            using var dbInfo = TestHelper.GetDisposableFile("db");
            await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
            await duckDBConnection.OpenAsync();

            await duckDBConnection.CloseAsync();
            await duckDBConnection.CloseAsync();
        }

        [TestMethod]
        [TestCategory("Baseline")]
        [ExpectedException(typeof(InvalidOperationException), "Did not throw expected exception")]
        public async Task ExceptionOnDisposeThenClose()
        {
            using var dbInfo = TestHelper.GetDisposableFile("db");
            await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
            await duckDBConnection.OpenAsync();

            await duckDBConnection.DisposeAsync();
            await duckDBConnection.CloseAsync();
        }

        /// <summary>
        /// Spin up set of tasks to randomly create connections, insert data, and close connections.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        [TestCategory("Long Running")]
        public async Task MultiThreadedStress()
        {
            //with 1 task per file, should be good mix of reusing connections
            //and disposing of them
            const int fileCount = 20;
            const int taskCount = 20;
            const int insertionCount = 1000;
            const int totalInsertions = taskCount * insertionCount;

            var files = new DisposableFile[fileCount];

            for (int i = 0; i < fileCount; i++)
            {
                files[i] = TestHelper.GetDisposableFile("db", i);
            }

            var connectionStrings = files.Select(f => $"DataSource={f.FileName}").ToArray();

            foreach (var cs in connectionStrings)
            {
                await using var duckDBConnection = new DuckDBConnection(cs);
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

                    var cs = connectionStrings[rnd.Next(connectionStrings.Length)];
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

            foreach (var cs in connectionStrings)
            {
                await using var duckDBConnection = new DuckDBConnection(cs);
                await duckDBConnection.OpenAsync();

                var insertions = await duckDBConnection.QuerySingleAsync<int>("SELECT COUNT(*) FROM INSERTIONS;");
                insertions.Should().BeGreaterThan(0);
                insertionCountPostRun += insertions;

                Console.WriteLine($"{insertions:0} Insertions for {cs}");
            }

            insertionCountPostRun.Should().Be(totalInsertions, $"Insertions don't add up?");

            //dispose here to make sure there isn't a connection still attached
            foreach (var f in files)
            {
                f.Dispose();
            }
        }

        [TestMethod]
        [TestCategory("Baseline")]
        public async Task NoExceptionOnDoubleDispose()
        {
            using var dbInfo = TestHelper.GetDisposableFile("db");
            await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
            await duckDBConnection.OpenAsync();

            await duckDBConnection.DisposeAsync();
            await duckDBConnection.DisposeAsync();
        }

        [TestMethod]
        [TestCategory("Baseline")]
        public async Task NoExceptionCloseThenDispose()
        {
            using var dbInfo = TestHelper.GetDisposableFile("db");
            await using var duckDBConnection = new DuckDBConnection(dbInfo.ConnectionString);
            await duckDBConnection.OpenAsync();

            await duckDBConnection.CloseAsync();
            await duckDBConnection.DisposeAsync();
        }

        [TestCategory("Baseline")]
        [TestMethod]
        public async Task SingleThreadedOpenAndCloseOfSameFile()
        {
            using var db1 = TestHelper.GetDisposableFile("db", 1);
            var connectionString = $"Data Source={db1.FileName}";

            await using var duckDBConnection = new DuckDBConnection(connectionString);
            await duckDBConnection.OpenAsync();

            var createTable = "CREATE TABLE INSERTIONS(TASK_ID INTEGER, INSERTION_INDEX INTEGER);";
            await duckDBConnection.ExecuteAsync(createTable);

            await using var dd1 = new DuckDBConnection(connectionString);
            await using var dd2 = new DuckDBConnection(connectionString);

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
    }
}
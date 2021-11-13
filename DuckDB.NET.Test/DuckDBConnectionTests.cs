using DuckDB.NET.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;
using Dapper;
using FluentAssertions;
using System.Linq;
using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test
{
    [TestClass]
    public class DuckDBConnectionTests
    {
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

        /// <summary>
        /// Spin up set of tasks to randomly create connections, insert data, and close connections.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        [TestCategory("Long Running")]
        public async Task ThreadingDisposal()
        {
            const int taskCount = 3;
            const int insertionCount = 300;
            const int totalInsertions = taskCount * insertionCount;

            //setup several DB's
            using var db1 = TestHelper.GetDisposableFile("db", 1);
            using var db2 = TestHelper.GetDisposableFile("db", 2);

            var files = new[] { db1, db2 };
            
            var connectionStrings = files.Select(f=>$"DataSource={f.FileName}").ToArray();

            foreach(var cs in connectionStrings)
            {
                await using var duckDBConnection = new DuckDBConnection(cs);
                await duckDBConnection.OpenAsync();

                var createTable = "CREATE TABLE INSERTIONS(TASK_ID INTEGER, INSERTION_INDEX INTEGER);";
                await duckDBConnection.ExecuteAsync(createTable);
            }
                        
            var insertionsWithRandomDelay = new Func<int, Task>(async ti => 
            {
                var rnd = new Random(ti);
                
                for(int i = 0; i < insertionCount; i++)
                {
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
            for(int i = 0; i< taskCount; i++)
            {
                tasks[i] = Task.Run(async () => await insertionsWithRandomDelay(i));
            }

            await Task.WhenAll(tasks);

            //sanity check of insertions and verification connections all closed
            int insertionCountPostRun = 0;

            foreach (var cs in connectionStrings)
            {
                await using var duckDBConnection = new DuckDBConnection(cs);
                await duckDBConnection.OpenAsync();

                var insertions = await duckDBConnection.QuerySingleAsync<int>("SELECT COUNT(*) FROM INSERTIONS;");
                insertionCountPostRun += insertions;
            }

            insertionCountPostRun.Should().Be(totalInsertions, $"Insertions don't add up?");

            //dispose here to make sure there isn't a connection still attached
            foreach(var f in files)
            {
                f.Dispose();
            }
        }
    }
}

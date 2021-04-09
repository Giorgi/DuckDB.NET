using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET.Data
{
    public class DuckDb
    {
        private DuckDBDatabase duckDBDatabase;
        public DuckDb(string connectionString)
        {
            if (connectionString.StartsWith("Data Source=") || connectionString.StartsWith("DataSource="))
            {
                var strings = connectionString.Split('=');

                if (strings[1] == ":memory:")
                {
                    InMemory = true;
                }
                else
                {
                    FilePath = strings[1];
                }
            }
            else
            {
                throw new DuckDBException("Invalid connection string");
            }

            var result = PlatformIndependentBindings.NativeMethods.DuckDBOpen(InMemory ? null : FilePath, out duckDBDatabase);
            if (!result.IsSuccess())
            {
                throw new DuckDBException("DuckDBOpen failed", result);
            }
        }

        internal DuckDBDatabase Database => duckDBDatabase;
        public string FilePath { get;}
        public bool InMemory { get; }
        public void Dispose()
        {
            duckDBDatabase.Dispose(); //TODO should this implement IDisposable?
        }
    }
}

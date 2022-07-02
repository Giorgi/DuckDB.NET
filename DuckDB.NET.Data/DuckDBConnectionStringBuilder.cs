using System;
using DuckDB.NET.Data.ConnectionString;

namespace DuckDB.NET.Data
{
    public class DuckDBConnectionStringBuilder : IDuckDBConnectionString
    {
        public const string InMemory = ":memory:";
        
        public string DataSource { get; set; }

        public override string ToString()
        {
            if (string.IsNullOrEmpty(DataSource))
            {
                throw new InvalidCastException("DataSource must be specified.");
            }

            return $"DataSource = {DataSource}";
        }
    }
}
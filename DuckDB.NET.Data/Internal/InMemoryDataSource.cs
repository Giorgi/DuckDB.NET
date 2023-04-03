using System;

namespace DuckDB.NET.Data.Internal
{
	internal static class InMemoryDataSource
	{
		public static readonly string Default = string.Empty;
		public static readonly string CacheShared = Guid.NewGuid().ToString();

		public static bool IsInMemoryDataSource(string dataSource)
		{
			return IsDefault(dataSource) || IsCacheShared(dataSource);
		}

		public static bool IsDefault(string dataSource)
		{
			return dataSource == Default;
		}

		public static bool IsCacheShared(string dataSource)
		{
			return string.Equals(dataSource, CacheShared, StringComparison.OrdinalIgnoreCase);
		}
	}
}

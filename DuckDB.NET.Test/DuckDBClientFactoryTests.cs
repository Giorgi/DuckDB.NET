using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DuckDB.NET.Test
{
    public class DuckDBClientFactoryTests
    {
        [Fact]
        public void RegisterFactory()
        {
            DbProviderFactories.RegisterFactory(DuckDBClientFactory.ProviderInvariantName, DuckDBClientFactory.Instance);
            DbProviderFactories.TryGetFactory(DuckDBClientFactory.ProviderInvariantName, out var factory);
            
            Assert.NotNull(factory);
            Assert.IsType<DuckDBClientFactory>(factory);
        }
    }
}

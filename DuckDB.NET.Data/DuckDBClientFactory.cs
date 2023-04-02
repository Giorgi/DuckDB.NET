using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace DuckDB.NET.Data
{
    public class DuckDBClientFactory : DbProviderFactory
    {
        public const string ProviderInvariantName = "DuckDB.NET.Data";

        #region Static Properties

        public static readonly DuckDBClientFactory Instance = new ();

        #endregion

        #region Properties

        public override bool CanCreateDataSourceEnumerator
        {
            get { return false; }
        }

        #endregion

        #region Constructors

        private DuckDBClientFactory()
            : base()
        { }

        #endregion

        #region Methods

        public override DbCommand CreateCommand()
            => new DuckDbCommand();

        public override DbCommandBuilder CreateCommandBuilder()
            => throw new NotImplementedException();

        public override DbConnection CreateConnection()
            => new DuckDBConnection();

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
            => new DuckDBConnectionStringBuilder();

        public override DbDataAdapter CreateDataAdapter()
            => throw new NotImplementedException();

        public override DbParameter CreateParameter()
            => new DuckDBParameter();

        #endregion
    }
}

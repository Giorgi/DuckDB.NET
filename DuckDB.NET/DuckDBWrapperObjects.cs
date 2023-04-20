using System;

using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDB.NET
{
    public class DuckDBDatabase : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBDatabase() : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Startup.DuckDBClose(out handle);
            return true;
        }
    }

    public class DuckDBNativeConnection : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBNativeConnection() : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Startup.DuckDBDisconnect(out handle);
            return true;
        }
    }

    public class DuckDBPreparedStatement : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBPreparedStatement() : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.PreparedStatements.DuckDBDestroyPrepare(out handle);
            return true;
        }
    }

    public class DuckDBConfig : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBConfig() : base(true)
        {

        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Configure.DuckDBDestroyConfig(out handle);
            return true;
        }
    }

    public class DuckDBAppender : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBAppender() : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            return NativeMethods.Appender.DuckDBDestroyAppender(out handle) == DuckDBState.DuckDBSuccess;
        }
    }

    public class DuckDBExtractedStatements : SafeHandleZeroOrMinusOneIsInvalid
    {
        public DuckDBExtractedStatements() : base(true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.ExtractStatements.DuckDBDestroyExtracted(out handle);

            return true;
        }
    }
}

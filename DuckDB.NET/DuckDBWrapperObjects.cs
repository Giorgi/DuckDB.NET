using System;

using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDB.NET
{
    public class DuckDBDatabase : SafeHandle
    {
        public DuckDBDatabase() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Startup.DuckDBClose(out handle);
            return true;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }

    public class DuckDBNativeConnection : SafeHandle
    {
        public DuckDBNativeConnection() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Startup.DuckDBDisconnect(out handle);
            return true;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }

    public class DuckDBPreparedStatement : SafeHandle
    {
        public DuckDBPreparedStatement() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.PreparedStatements.DuckDBDestroyPrepare(out handle);
            return true;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }

    public class DuckDBConfig : SafeHandle
    {
        public DuckDBConfig(): base(IntPtr.Zero, true)
        {
            
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.Configure.DuckDBDestroyConfig(out handle);
            return true;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }

    public class DuckDBAppender : SafeHandle
    {
        public DuckDBAppender() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            return NativeMethods.Appender.DuckDBDestroyAppender(out handle) == DuckDBState.DuckDBSuccess;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
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

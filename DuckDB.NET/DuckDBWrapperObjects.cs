using System;
using System.Runtime.InteropServices;

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
}
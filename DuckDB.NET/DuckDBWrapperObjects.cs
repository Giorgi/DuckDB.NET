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
            NativeMethods.DuckDBClose(out handle);
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
            NativeMethods.DuckDBDisconnect(out handle);
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
            NativeMethods.DuckDBDestroyPrepare(out handle);
            return true;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}
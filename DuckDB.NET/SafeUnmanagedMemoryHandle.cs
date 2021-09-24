using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDB.NET
{
    public class SafeUnmanagedMemoryHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        public SafeUnmanagedMemoryHandle() : base(true) { }

        public SafeUnmanagedMemoryHandle(IntPtr preexistingHandle, bool ownsHandle) : base(ownsHandle)
        {
            SetHandle(preexistingHandle);
        }

        protected override bool ReleaseHandle()
        {
            if (handle != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(handle);

                handle = IntPtr.Zero;

                return true;
            }

            return false;
        }
    }
}
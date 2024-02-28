using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DuckDB.NET.Native;

public class SafeUnmanagedMemoryHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly bool freeWithGlobal;
    public SafeUnmanagedMemoryHandle() : base(true) { }

    public SafeUnmanagedMemoryHandle(IntPtr preexistingHandle, bool ownsHandle, bool freeWithGlobal = true) : base(ownsHandle)
    {
        this.freeWithGlobal = freeWithGlobal;
        SetHandle(preexistingHandle);
    }

    protected override bool ReleaseHandle()
    {
        if (handle != IntPtr.Zero)
        {
            if (freeWithGlobal)
            {
                Marshal.FreeHGlobal(handle);
            }
            else
            {
                Marshal.FreeCoTaskMem(handle);
            }

            handle = IntPtr.Zero;

            return true;
        }

        return false;
    }
}
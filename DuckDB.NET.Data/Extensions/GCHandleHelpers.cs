using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data.Extensions;

internal static class GCHandleHelpers
{
    internal static IntPtr ToHandle(this object item) => GCHandle.ToIntPtr(GCHandle.Alloc(item));

    internal static void FreeHandle(this IntPtr pointer)
    {
        var handle = GCHandle.FromIntPtr(pointer);
        (handle.Target as IDisposable)?.Dispose();
        handle.Free();
    }
}
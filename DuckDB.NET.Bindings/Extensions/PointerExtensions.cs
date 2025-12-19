using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET.Native;

public static class PointerExtensions
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static unsafe string ToManagedString(this IntPtr unmanagedString, bool freeWhenCopied = true, int? length = null)
    {
        if (unmanagedString == IntPtr.Zero)
        {
            return string.Empty;
        }

        var span = length.HasValue ? new ReadOnlySpan<byte>((byte*)unmanagedString, length.Value) 
                                   : MemoryMarshal.CreateReadOnlySpanFromNullTerminated((byte*)unmanagedString);

        var result = Encoding.UTF8.GetString(span);

        if (freeWhenCopied)
        {
            NativeMethods.Helpers.DuckDBFree(unmanagedString);
        }

        return result;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public static SafeUnmanagedMemoryHandle ToUnmanagedString(this string? managedString) => new(Marshal.StringToCoTaskMemUTF8(managedString));
}
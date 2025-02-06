using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET.Native;

public static class PointerExtensions
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ToManagedString(this IntPtr unmanagedString, bool freeWhenCopied = true, int? length = null)
    {
        string result;
#if NET6_0_OR_GREATER
        result = length.HasValue
            ? Marshal.PtrToStringUTF8(unmanagedString, length.Value)
            : Marshal.PtrToStringUTF8(unmanagedString) ?? string.Empty;
#else
        if (unmanagedString == IntPtr.Zero)
        {
            return "";
        }

        if (length == null)
        {
            length = 0;

            while (Marshal.ReadByte(unmanagedString, length.Value) != 0)
            {
                length++;
            }
        }

        if (length == 0)
        {
            return string.Empty;
        }

        var byteArray = new byte[length.Value];

        Marshal.Copy(unmanagedString, byteArray, 0, length.Value);

        result = Encoding.UTF8.GetString(byteArray, 0, length.Value);
#endif
        if (freeWhenCopied)
        {
            NativeMethods.Helpers.DuckDBFree(unmanagedString);
        }

        return result;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public static SafeUnmanagedMemoryHandle ToUnmanagedString(this string? managedString)
    {
#if NET6_0_OR_GREATER
        var pointer = Marshal.StringToCoTaskMemUTF8(managedString);
        return new SafeUnmanagedMemoryHandle(pointer);
#else
        
        if (managedString == null)
        {
            return new SafeUnmanagedMemoryHandle();
        }

        int len = Encoding.UTF8.GetByteCount(managedString);

        var buffer = new byte[len + 1];
        Encoding.UTF8.GetBytes(managedString, 0, managedString.Length, buffer, 0);

        var nativeUtf8 = Marshal.AllocCoTaskMem(buffer.Length);
        Marshal.Copy(buffer, 0, nativeUtf8, buffer.Length);

        return new SafeUnmanagedMemoryHandle(nativeUtf8); 
#endif
    }
}
using System;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET
{
    public static class Utils
    {
        public static bool IsSuccess(this DuckDBState duckDBState)
        {
            return duckDBState == DuckDBState.DuckDBSuccess;
        }

        public static string ToManagedString(this IntPtr unmanagedString, bool freeWhenCopied = true)
        {
#if NET6_0_OR_GREATER
            return Marshal.PtrToStringUTF8(unmanagedString);
#else
            if (unmanagedString == IntPtr.Zero)
            {
                return "";
            }

            var length = 0;

            while (Marshal.ReadByte(unmanagedString, length) != 0)
            {
                length++;
            }

            if (length == 0)
            {
                return string.Empty;
            }

            var byteArray = new byte[length];

            Marshal.Copy(unmanagedString, byteArray, 0, length);

            if (freeWhenCopied)
            {
                NativeMethods.Helpers.DuckDBFree(unmanagedString);
            }

            return Encoding.UTF8.GetString(byteArray, 0, length);
#endif
        }

        public static SafeUnmanagedMemoryHandle ToUnmanagedString(this string managedString)
        {
#if NET6_0_OR_GREATER
            var pointer = Marshal.StringToCoTaskMemUTF8(managedString);

            return new SafeUnmanagedMemoryHandle(pointer, true, false);
#else
            if (managedString == null)
            {
                return new SafeUnmanagedMemoryHandle(IntPtr.Zero, true);
            }

            int len = Encoding.UTF8.GetByteCount(managedString);

            var buffer = new byte[len + 1];
            Encoding.UTF8.GetBytes(managedString, 0, managedString.Length, buffer, 0);

            var nativeUtf8 = Marshal.AllocHGlobal(buffer.Length);
            Marshal.Copy(buffer, 0, nativeUtf8, buffer.Length);

            return new SafeUnmanagedMemoryHandle(nativeUtf8, true);
#endif
        }
    }
}

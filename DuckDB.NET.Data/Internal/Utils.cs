using System;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET.Data.Internal
{
    static class Utils
    {
        internal static bool IsSuccess(this DuckDBState duckDBState)
        {
            return duckDBState == DuckDBState.DuckDBSuccess;
        }

        internal static string ToManagedString(this IntPtr unmanagedString)
        {
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

            PlatformIndependentBindings.NativeMethods.DuckDBFree(unmanagedString);

            return Encoding.UTF8.GetString(byteArray, 0, length);
        }

        internal static SafeUnmanagedMemoryHandle ToUnmanagedString(this string managedString)
        {
            int len = Encoding.UTF8.GetByteCount(managedString);

            var buffer = new byte[len + 1];
            Encoding.UTF8.GetBytes(managedString, 0, managedString.Length, buffer, 0);

            var nativeUtf8 = Marshal.AllocHGlobal(buffer.Length);
            Marshal.Copy(buffer, 0, nativeUtf8, buffer.Length);

            return new SafeUnmanagedMemoryHandle(nativeUtf8, true);
        }
    }
}
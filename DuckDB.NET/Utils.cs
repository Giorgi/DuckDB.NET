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
            if (unmanagedString == IntPtr.Zero)
            {
                return "";
            }

            var result = Marshal.PtrToStringUTF8(unmanagedString);

            if (freeWhenCopied)
            {
                NativeMethods.Helpers.DuckDBFree(unmanagedString);
            }

            return result;
        }

        public static SafeUnmanagedMemoryHandle ToUnmanagedString(this string managedString)
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

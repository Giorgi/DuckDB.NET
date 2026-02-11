using System.ComponentModel;

namespace DuckDB.NET.Native;

public static class PointerExtensions
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static string ToManagedString(this IntPtr unmanagedString, bool freeWhenCopied = true, int? length = null)
    {
        var result = length == null ? Marshal.PtrToStringUTF8(unmanagedString) : Marshal.PtrToStringUTF8(unmanagedString, length.Value);
        
        if (freeWhenCopied)
        {
            NativeMethods.Helpers.DuckDBFree(unmanagedString);
        }

        return result;
    }
}
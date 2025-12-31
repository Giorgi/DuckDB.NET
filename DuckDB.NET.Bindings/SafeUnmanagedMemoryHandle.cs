namespace DuckDB.NET.Native;

public class SafeUnmanagedMemoryHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    public SafeUnmanagedMemoryHandle() : base(true) { }

    public SafeUnmanagedMemoryHandle(IntPtr preexistingHandle) : base(true)
    {
        SetHandle(preexistingHandle);
    }

    protected override bool ReleaseHandle()
    {
        if (handle != IntPtr.Zero)
        {
            Marshal.FreeCoTaskMem(handle);

            handle = IntPtr.Zero;

            return true;
        }

        return false;
    }
}
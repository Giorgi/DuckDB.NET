using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Native;

// SafeHandles for the long-lived handles of the C API V2, over the generated bindings in NativeMethods/V2/Generated.
// Borrowed handles (vectors, chunks inside a callback, error infos) stay plain pointers: a SafeHandle would add a
// reference count to every call on the hot paths for no gain.

/// <summary>
/// A V2 handle that owns a <typeparamref name="T"/>. Every V2 destructor takes the handle by reference and sets it
/// to null.
/// </summary>
internal abstract unsafe class DuckDBV2SafeHandle<T> : SafeHandleZeroOrMinusOneIsInvalid where T : unmanaged
{
    protected DuckDBV2SafeHandle(T* pointer) : base(true) => SetHandle((IntPtr)pointer);

    /// <summary>
    /// The pointer to pass to a V2 call. The handle cannot be released, by Dispose or by the finalizer, until the
    /// lease is disposed. Throws <see cref="ObjectDisposedException"/> when the handle is already closed.
    /// </summary>
    public Lease Acquire() => new(this);

    protected abstract DUCKDB_V2_ERROR Destroy(T** pointer);

    protected override bool ReleaseHandle()
    {
        var pointer = (T*)handle;
        return Destroy(&pointer) == DUCKDB_V2_ERROR.NONE;
    }

    public readonly ref struct Lease : IDisposable
    {
        private readonly DuckDBV2SafeHandle<T> owner;

        internal Lease(DuckDBV2SafeHandle<T> owner)
        {
            var added = false;
            owner.DangerousAddRef(ref added);
            this.owner = owner;
            Pointer = (T*)owner.DangerousGetHandle();
        }

        public T* Pointer { get; }

        public void Dispose() => owner.DangerousRelease();
    }
}

/// <summary>
/// The root of the V2 API. Its destructor refuses while an instance created through it is still alive, so an
/// environment is best kept for the lifetime of the process.
/// </summary>
internal sealed unsafe class DuckDBV2Environment(_duckdb_v2_environment* pointer) : DuckDBV2SafeHandle<_duckdb_v2_environment>(pointer)
{
    protected override DUCKDB_V2_ERROR Destroy(_duckdb_v2_environment** pointer) => NativeMethodsV2.duckdb_v2_environment_destroy(pointer);
}

/// <summary>An instance, which holds the attached databases. Its connections keep it alive after it is destroyed.</summary>
internal sealed unsafe class DuckDBV2Instance(_duckdb_v2_instance* pointer) : DuckDBV2SafeHandle<_duckdb_v2_instance>(pointer)
{
    protected override DUCKDB_V2_ERROR Destroy(_duckdb_v2_instance** pointer) => NativeMethodsV2.duckdb_v2_instance_destroy(pointer);
}

internal sealed unsafe class DuckDBV2Connection(_duckdb_v2_connection* pointer) : DuckDBV2SafeHandle<_duckdb_v2_connection>(pointer)
{
    protected override DUCKDB_V2_ERROR Destroy(_duckdb_v2_connection** pointer) => NativeMethodsV2.duckdb_v2_connection_destroy(pointer);
}

/// <summary>A result. Destroying it before its end aborts the statement.</summary>
internal sealed unsafe class DuckDBV2Result(_duckdb_v2_result* pointer) : DuckDBV2SafeHandle<_duckdb_v2_result>(pointer)
{
    protected override DUCKDB_V2_ERROR Destroy(_duckdb_v2_result** pointer) => NativeMethodsV2.duckdb_v2_result_destroy(pointer);
}

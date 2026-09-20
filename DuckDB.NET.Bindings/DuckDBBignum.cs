namespace DuckDB.NET.Native;

/// <summary>
/// The absolute value of a BIGNUM (VARINT), as big endian bytes, plus its sign.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBBignum(IntPtr data, ulong size, byte isNegative)
{
    public IntPtr Data { get; } = data;
    public ulong Size { get; } = size;
    public byte IsNegative { get; } = isNegative;
}

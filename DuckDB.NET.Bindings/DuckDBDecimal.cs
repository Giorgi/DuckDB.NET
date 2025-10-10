using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBDecimal(byte width, byte scale, DuckDBHugeInt value)
{
    public byte Width { get; } = width;
    public byte Scale { get; } = scale;

    public DuckDBHugeInt Value { get; } = value;
}
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBDecimal
{
    public byte Width { get; }
    public byte Scale { get; }

    public DuckDBHugeInt Value { get; }
}
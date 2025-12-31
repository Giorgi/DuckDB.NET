namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class StringVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendString(string value, ulong rowIndex)
    {
        using var unmanagedString = value.ToUnmanagedString();
        NativeMethods.Vectors.DuckDBVectorAssignStringElement(Vector, rowIndex, unmanagedString);
        return true;
    }

    internal override bool AppendBlob(byte* value, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(Vector, rowIndex, value, length);
        return true;
    }
}

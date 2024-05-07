namespace DuckDB.NET.Data.Reader;

public interface IDuckDBDataReader
{
    unsafe bool IsValid(ulong offset);
    T GetValue<T>(ulong offset);
}
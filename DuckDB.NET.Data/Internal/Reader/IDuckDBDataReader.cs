namespace DuckDB.NET.Data.Reader;

public interface IDuckDBDataReader
{
    bool IsValid(ulong offset);
    T GetValue<T>(ulong offset);
}
namespace DuckDB.NET.Native;

public interface IDuckDBValueReader
{
    T GetValue<T>();
}
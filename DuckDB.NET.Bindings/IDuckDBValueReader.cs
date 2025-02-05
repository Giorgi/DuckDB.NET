namespace DuckDB.NET.Native;

public interface IDuckDBValueReader
{
    bool IsNull();
    T GetValue<T>();
}
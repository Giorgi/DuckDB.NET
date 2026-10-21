namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class BooleanVectorDataReader : VectorDataReaderBase
{
    private static readonly object True = true;
    private static readonly object False = false;

    internal unsafe BooleanVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        if (columnType == DuckDBType.Boolean)
        {
            StorageType = typeof(bool);
        }
    }

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Boolean)
        {
            return base.GetValidValue<T>(offset);
        }

        var value = GetFieldData<bool>(offset);
        return (T)(object)value; //JIT will optimize the casts at least for not nullable T
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Boolean)
        {
            return base.GetValueCore(offset, targetType);
        }

        // A boolean has two values, so the two boxes are made once and shared.
        return GetFieldData<bool>(offset) ? True : False;
    }
}
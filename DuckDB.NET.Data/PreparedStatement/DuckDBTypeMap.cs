namespace DuckDB.NET.Data.PreparedStatement;

internal static class DuckDBTypeMap
{
    private static readonly Dictionary<Type, DbType> ClrToDbTypeMap = new()
    {
        {typeof(bool), DbType.Boolean},
        {typeof(sbyte), DbType.SByte},
        {typeof(short), DbType.Int16},
        {typeof(int), DbType.Int32},
        {typeof(long), DbType.Int64},
        {typeof(float), DbType.Single},
        {typeof(double), DbType.Double},
        {typeof(string), DbType.String},
        {typeof(Guid), DbType.Guid},
        {typeof(decimal), DbType.Currency},
        {typeof(byte), DbType.Byte},
        {typeof(ushort), DbType.UInt16},
        {typeof(uint), DbType.UInt32},
        {typeof(ulong), DbType.UInt64},
        {typeof(BigInteger), DbType.VarNumeric},
        {typeof(byte[]), DbType.Binary},
        {typeof(DateTime), DbType.DateTime},
        {typeof(DuckDBTimestamp), DbType.DateTime},
        {typeof(DateTimeOffset), DbType.DateTimeOffset},
        {typeof(DuckDBDateOnly), DbType.Date},
        {typeof(DuckDBTimeOnly), DbType.Time},
        {typeof(DateOnly), DbType.Date},
        {typeof(TimeOnly), DbType.Time},
    };

    public static DbType GetDbTypeForValue(object? value)
    {
        if (value.IsNull())
        {
            return DbType.Object;
        }

        var type = value!.GetType();

        return ClrToDbTypeMap.GetValueOrDefault(type, DbType.Object);
    }
}

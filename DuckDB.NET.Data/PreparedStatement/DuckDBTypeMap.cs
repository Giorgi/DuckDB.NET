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
        {typeof(decimal), DbType.Decimal},
        {typeof(byte), DbType.Byte},
        {typeof(ushort), DbType.UInt16},
        {typeof(uint), DbType.UInt32},
        {typeof(ulong), DbType.UInt64},
        {typeof(BigInteger), DbType.VarNumeric},
        {typeof(Int128), DbType.VarNumeric},
        {typeof(UInt128), DbType.VarNumeric},
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

    // The CLR type an assigned DbType converts a value to (DbTypeConverter); null for Object and for a DbType that
    // converts nothing.
    private static readonly Dictionary<DbType, Type> DbTypeToClrMap = new()
    {
        {DbType.SByte, typeof(sbyte)},
        {DbType.Int16, typeof(short)},
        {DbType.Int32, typeof(int)},
        {DbType.Int64, typeof(long)},
        {DbType.Byte, typeof(byte)},
        {DbType.UInt16, typeof(ushort)},
        {DbType.UInt32, typeof(uint)},
        {DbType.UInt64, typeof(ulong)},
        {DbType.VarNumeric, typeof(BigInteger)},
        {DbType.Single, typeof(float)},
        {DbType.Double, typeof(double)},
        {DbType.Decimal, typeof(decimal)},
        {DbType.Currency, typeof(decimal)},
        {DbType.String, typeof(string)},
        {DbType.AnsiString, typeof(string)},
        {DbType.StringFixedLength, typeof(string)},
        {DbType.AnsiStringFixedLength, typeof(string)},
        {DbType.Xml, typeof(string)},
        {DbType.Boolean, typeof(bool)},
        {DbType.Guid, typeof(Guid)},
        {DbType.Binary, typeof(byte[])},
        {DbType.Date, typeof(DateOnly)},
        {DbType.Time, typeof(TimeOnly)},
        {DbType.DateTime, typeof(DateTime)},
        {DbType.DateTime2, typeof(DateTime)},
        {DbType.DateTimeOffset, typeof(DateTimeOffset)},
    };

    public static Type? GetClrType(DbType dbType) => DbTypeToClrMap.GetValueOrDefault(dbType);
}

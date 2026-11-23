using System.Linq;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// The public V1 enums for what V2 reports with its own numbers: V2's logical type ids are the engine's
/// (VARCHAR = 25), and <see cref="DuckDBType"/> keeps V1's (VARCHAR = 17).
/// </summary>
internal static class DuckDBV2TypeMap
{
    public static DuckDBType ToDuckDBType(this DUCKDB_V2_LOGICAL_TYPE_ID id) => id switch
    {
        DUCKDB_V2_LOGICAL_TYPE_ID.BOOLEAN => DuckDBType.Boolean,
        DUCKDB_V2_LOGICAL_TYPE_ID.TINYINT => DuckDBType.TinyInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.SMALLINT => DuckDBType.SmallInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER => DuckDBType.Integer,
        DUCKDB_V2_LOGICAL_TYPE_ID.BIGINT => DuckDBType.BigInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.UTINYINT => DuckDBType.UnsignedTinyInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.USMALLINT => DuckDBType.UnsignedSmallInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.UINTEGER => DuckDBType.UnsignedInteger,
        DUCKDB_V2_LOGICAL_TYPE_ID.UBIGINT => DuckDBType.UnsignedBigInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.FLOAT => DuckDBType.Float,
        DUCKDB_V2_LOGICAL_TYPE_ID.DOUBLE => DuckDBType.Double,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP => DuckDBType.Timestamp,
        DUCKDB_V2_LOGICAL_TYPE_ID.DATE => DuckDBType.Date,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIME => DuckDBType.Time,
        DUCKDB_V2_LOGICAL_TYPE_ID.INTERVAL => DuckDBType.Interval,
        DUCKDB_V2_LOGICAL_TYPE_ID.HUGEINT => DuckDBType.HugeInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.UHUGEINT => DuckDBType.UnsignedHugeInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.VARCHAR => DuckDBType.Varchar,
        DUCKDB_V2_LOGICAL_TYPE_ID.BLOB => DuckDBType.Blob,
        DUCKDB_V2_LOGICAL_TYPE_ID.DECIMAL => DuckDBType.Decimal,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_SEC => DuckDBType.TimestampS,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_MS => DuckDBType.TimestampMs,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_NS => DuckDBType.TimestampNs,
        DUCKDB_V2_LOGICAL_TYPE_ID.ENUM => DuckDBType.Enum,
        DUCKDB_V2_LOGICAL_TYPE_ID.LIST => DuckDBType.List,
        DUCKDB_V2_LOGICAL_TYPE_ID.ARRAY => DuckDBType.Array,
        DUCKDB_V2_LOGICAL_TYPE_ID.STRUCT => DuckDBType.Struct,
        DUCKDB_V2_LOGICAL_TYPE_ID.MAP => DuckDBType.Map,
        DUCKDB_V2_LOGICAL_TYPE_ID.UUID => DuckDBType.Uuid,
        DUCKDB_V2_LOGICAL_TYPE_ID.UNION => DuckDBType.Union,
        DUCKDB_V2_LOGICAL_TYPE_ID.BIT => DuckDBType.Bit,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIME_TZ => DuckDBType.TimeTz,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_TZ => DuckDBType.TimestampTz,
        DUCKDB_V2_LOGICAL_TYPE_ID.ANY => DuckDBType.Any,
        DUCKDB_V2_LOGICAL_TYPE_ID.BIGNUM => DuckDBType.VarInt,
        DUCKDB_V2_LOGICAL_TYPE_ID.SQLNULL => DuckDBType.SqlNull,
        DUCKDB_V2_LOGICAL_TYPE_ID.TIME_NS => DuckDBType.TimeNs,
        DUCKDB_V2_LOGICAL_TYPE_ID.GEOMETRY => DuckDBType.Geometry,
        DUCKDB_V2_LOGICAL_TYPE_ID.VARIANT => DuckDBType.Variant,
        // TIMESTAMP_TZ_NS, GEOGRAPHY, TUPLE, TYPE, UNKNOWN: V1 has no DuckDBType for them.
        _ => DuckDBType.Invalid,
    };

    // The statement types have the same names in both APIs but different numbers (V1 puts EXPLAIN at 4, V2 at 14).
    private static readonly Dictionary<DUCKDB_V2_STATEMENT_TYPE, DuckDBStatementType> StatementTypes = BuildStatementTypes();

    public static DuckDBStatementType ToStatementType(this DUCKDB_V2_STATEMENT_TYPE type) =>
        StatementTypes.TryGetValue(type, out var statementType) ? statementType : DuckDBStatementType.Invalid;

    private static Dictionary<DUCKDB_V2_STATEMENT_TYPE, DuckDBStatementType> BuildStatementTypes()
    {
        var byName = Enum.GetValues<DuckDBStatementType>().ToDictionary(type => type.ToString(), StringComparer.OrdinalIgnoreCase);
        var types = new Dictionary<DUCKDB_V2_STATEMENT_TYPE, DuckDBStatementType>();

        foreach (var type in Enum.GetValues<DUCKDB_V2_STATEMENT_TYPE>())
        {
            // V2 has statement types V1 does not (CONNECT, PASSTHROUGH, ...): they report Invalid.
            if (byName.TryGetValue(type.ToString().Replace("_", ""), out var statementType))
            {
                types[type] = statementType;
            }
        }

        return types;
    }
}

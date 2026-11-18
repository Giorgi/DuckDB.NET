using DuckDB.NET.Data.PreparedStatement;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    private object? value;

    // With no value there is no type to infer: Object, as in Npgsql.
    private DbType dbType = DbType.Object;

    // Once a DbType is assigned it stays: assigning Value afterwards must not replace it with the type inferred from the value.
    // Until then (and after ResetDbType) the type follows the value, as it does for SqlParameter and NpgsqlParameter.
    private bool inferDbType = true;

    public override DbType DbType
    {
        get => dbType;
        set
        {
            dbType = value;
            inferDbType = false;
        }
    }

    [AllowNull]
    [DefaultValue("")]
    public override string ParameterName { get; set; }

    public override object? Value
    {
        get => value;
        set
        {
            if (this.value != value)
            {
                this.value = value;

                if (inferDbType)
                {
                    dbType = DuckDBTypeMap.GetDbTypeForValue(value);
                }
            }
        }
    }

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }

    [AllowNull]
    [DefaultValue("")]
    public override string SourceColumn { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public DuckDBParameter()
        : this(string.Empty, DbType.Object, null, inferDbType: true)
    { }

    public DuckDBParameter(object value) 
        : this(string.Empty, DuckDBTypeMap.GetDbTypeForValue(value), value, inferDbType: true)
    {
    }

    public DuckDBParameter(DbType type, object? value) 
        : this(string.Empty, type, value)
    {
    }

    public DuckDBParameter(string name, object? value) 
        : this(name, DuckDBTypeMap.GetDbTypeForValue(value), value, inferDbType: true)
    {
    }

    public DuckDBParameter(string name, DbType type, object? value)
        : this(name, type, value, inferDbType: false)
    {
    }

    private DuckDBParameter(string name, DbType type, object? value, bool inferDbType)
    {
        dbType = type;
        this.inferDbType = inferDbType;
        ParameterName = name;
        this.value = value;
        SourceColumn = string.Empty;
    }

    // Back to the type of the current value, as if no DbType had been assigned.
    public override void ResetDbType()
    {
        dbType = DuckDBTypeMap.GetDbTypeForValue(value);
        inferDbType = true;
    }
}
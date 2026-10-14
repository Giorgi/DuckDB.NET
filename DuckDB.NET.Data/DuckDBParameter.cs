using DuckDB.NET.Data.PreparedStatement;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    private const DbType DefaultDbType = DbType.Object;

    private object? value;
    private string parameterName = string.Empty;
    private string sourceColumn = string.Empty;

    public override DbType DbType { get; set; }

    [AllowNull]
    [DefaultValue("")]
    public override string ParameterName
    {
        get => parameterName;
        set => parameterName = value ?? string.Empty;
    }

    public override object? Value
    {
        get => value;
        set
        {
            if (this.value != value)
            {
                this.value = value;
                DbType = DuckDBTypeMap.GetDbTypeForValue(value);
            }
        }
    }

    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }

    [AllowNull]
    [DefaultValue("")]
    public override string SourceColumn
    {
        get => sourceColumn;
        set => sourceColumn = value ?? string.Empty;
    }
    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public DuckDBParameter()
        : this (string.Empty, DefaultDbType, null)
    { }

    public DuckDBParameter(object value) 
        : this(string.Empty, DuckDBTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(DbType type, object? value) 
        : this(string.Empty, type, value)
    {
    }

    public DuckDBParameter(string name, object? value) 
        : this(name, DuckDBTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(string name, DbType type, object? value)
    {
        DbType = type;
        ParameterName = name;
        this.value = value;
        SourceColumn = string.Empty;
    }

    public override void ResetDbType() => DbType = DefaultDbType;
}

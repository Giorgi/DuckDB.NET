using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using DuckDB.NET.Data.Internal;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    private const DbType DEFAULT_DB_TYPE = DbType.String;

    private object? value;

    public override DbType DbType { get; set; }

#if NET6_0_OR_GREATER
    [AllowNull]
#endif
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
                DbType = DbTypeMap.GetDbTypeForValue(value);
            }
        }
    }


    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }

#if NET6_0_OR_GREATER
    [AllowNull]
#endif
    [DefaultValue("")]
    public override string SourceColumn { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public DuckDBParameter()
        : this (string.Empty, DEFAULT_DB_TYPE, null)
    { }

    public DuckDBParameter(object value) 
        : this(string.Empty, DbTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(DbType type, object? value) 
        : this(string.Empty, type, value)
    {
    }

    public DuckDBParameter(string name, object? value) 
        : this(name, DbTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(string name, DbType type, object? value)
    {
        DbType = type;
        ParameterName = name;
        this.value = value;
        SourceColumn = string.Empty;
    }

    public override void ResetDbType()
        => DbType = DEFAULT_DB_TYPE;
}
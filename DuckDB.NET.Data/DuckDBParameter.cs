using System;
using System.Data;
using System.Data.Common;
using DuckDB.NET.Data.Internal;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    private object value;

    public override DbType DbType { get; set; }

    public override string ParameterName { get; set; }

    public override object Value
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
    public override string SourceColumn { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public DuckDBParameter()
    {

    }

    public DuckDBParameter(object value) : this("", DbTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(DbType type, object value) : this("", type, value)
    {
    }

    public DuckDBParameter(string name, object value) : this(name, DbTypeMap.GetDbTypeForValue(value), value)
    {
    }

    public DuckDBParameter(string name, DbType type, object value)
    {
        DbType = type;
        ParameterName = name;
        this.value = value;
    }

    public override void ResetDbType()
    {
        DbType = DbType.String;
    }
}
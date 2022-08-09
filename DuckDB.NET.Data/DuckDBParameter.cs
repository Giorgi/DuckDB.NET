using System;
using System.Data;
using System.Data.Common;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Data.Types;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    private DbType dbType;
    private string parameterName;
    private object value;

    public override DbType DbType
    {
        get => dbType;
        set => dbType = value;
    }

    public override string ParameterName
    {
        get => parameterName;
        set => parameterName = value;
    }

    public override object Value
    {
        get => value;
        set
        {
            if (this.value != value)
            {
                this.value = value;
                dbType = DbTypeMap.GetDbTypeForValue(value);
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

    private DuckDBParameter(string name, DbType type, object value)
    {
        parameterName = name;
        dbType = type;
        this.value = value;
    }

    public DuckDBParameter(DbType type, object value)
    {
        dbType = type;
        this.value = value;
    }

    public DuckDBParameter(object value)
    {
        dbType = DbTypeMap.GetDbTypeForValue(value);
        this.value = value;
    }

    public DuckDBParameter(bool value)
        : this(DbType.Boolean, value)
    {
    }

    public DuckDBParameter(sbyte value)
        : this(DbType.SByte, value)
    {
    }

    public DuckDBParameter(short value)
        : this(DbType.Int16, value)
    {
    }

    public DuckDBParameter(int value)
        : this(DbType.Int32, value)
    {
    }

    public DuckDBParameter(long value)
        : this(DbType.Int64, value)
    {
    }

    public DuckDBParameter(float value)
        : this(DbType.Single, value)
    {
    }

    public DuckDBParameter(double value)
        : this(DbType.Double, value)
    {
    }

    public DuckDBParameter(string value)
        : this(DbType.String, value)
    {
    }

    public DuckDBParameter(string name, bool value)
        : this(name, DbType.Boolean, value)
    {
    }

    public DuckDBParameter(string name, sbyte value)
        : this(name, DbType.SByte, value)
    {
    }

    public DuckDBParameter(string name, short value)
        : this(name, DbType.Int16, value)
    {
    }

    public DuckDBParameter(string name, int value)
        : this(name, DbType.Int32, value)
    {
    }

    public DuckDBParameter(string name, long value)
        : this(name, DbType.Int64, value)
    {
    }

    public DuckDBParameter(string name, float value)
        : this(name, DbType.Single, value)
    {
    }

    public DuckDBParameter(string name, double value)
        : this(name, DbType.Double, value)
    {
    }

    public DuckDBParameter(string name, string value)
        : this(name, DbType.String, value)
    {
    }

    public DuckDBParameter(string name, DateTime value)
        : this(name, DbType.DateTime, Types.DuckDBTimestamp.FromDateTime(value))
    {
    }
    
    public DuckDBParameter(DateTime value)
        : this(DbType.DateTime, Types.DuckDBTimestamp.FromDateTime(value))
    {
    }

    public override void ResetDbType()
    {
        DbType = DbType.String;
    }
}
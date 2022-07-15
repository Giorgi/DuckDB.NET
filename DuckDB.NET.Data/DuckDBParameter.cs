using System.Data;
using System.Data.Common;

namespace DuckDB.NET.Data;

public class DuckDBParameter : DbParameter
{
    public override DbType DbType { get; set; } = DbType.String;
    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; }
    public override string SourceColumn { get; set; }
    public override object Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public DuckDBParameter()
    {
        
    }

    public DuckDBParameter(string name, object? value)
    {
        ParameterName = name;
        Value = value;
    }
    
    public DuckDBParameter(object? value)
    {
        Value = value;
    }
    
    public override void ResetDbType()
    {
        DbType = DbType.String;
    }
}
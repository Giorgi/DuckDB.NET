using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace DuckDB.NET.Data;

public class DuckDBParameterCollection : DbParameterCollection
{
    private readonly List<DuckDBParameter> parameters = new();

    public new DuckDBParameter this[int index]
    {
        get => parameters[index];
        set => parameters[index] = value;
    }

    public new DuckDBParameter this[string parameterName]
    {
        get => this[IndexOfSafe(parameterName)];
        set => this[IndexOfSafe(parameterName)] = value;
    }
    
    public override int Count => parameters.Count;
    public override object SyncRoot => ((ICollection)parameters).SyncRoot;
    
    public override int Add(object value)
    {
        parameters.Add((DuckDBParameter)value);
        return parameters.Count - 1;
    }

    public override void Clear() => parameters.Clear();

    public override bool Contains(object value) => parameters.Contains((DuckDBParameter) value);

    public override int IndexOf(object value) => parameters.IndexOf((DuckDBParameter) value);

    public override void Insert(int index, object value) => parameters.Insert(index, (DuckDBParameter) value);

    public override void Remove(object value) => parameters.Remove((DuckDBParameter) value);

    public int Add(DuckDBParameter value)
    {
        parameters.Add(value);
        return parameters.Count - 1;
    }
    
    public bool Contains(DuckDBParameter value) => parameters.Contains(value);

    public int IndexOf(DuckDBParameter value) => parameters.IndexOf(value);

    public void Insert(int index, DuckDBParameter value) => parameters.Insert(index, value);

    public void Remove(DuckDBParameter value) => parameters.Remove(value);

    
    public override void RemoveAt(int index) => parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOfSafe(parameterName);
        parameters.RemoveAt(index);
    }

    protected override void SetParameter(int index, DbParameter value)
        => parameters[index] = (DuckDBParameter)value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOfSafe(parameterName);
        parameters[index] = (DuckDBParameter)value;
    }

    public override int IndexOf(string parameterName)
        => parameters.FindIndex(p => p.ParameterName.Equals(parameterName, StringComparison.Ordinal));

    public override bool Contains(string value)
        => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index)
        => parameters.CopyTo((DuckDBParameter[])array, index);
    
    public void CopyTo(DuckDBParameter[] array, int index)
        => parameters.CopyTo(array, index);

    public override IEnumerator GetEnumerator() => parameters.GetEnumerator();

    protected override DbParameter GetParameter(int index) => parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        return parameters[index];
    }

    public override void AddRange(Array values)
        => AddRange(values.Cast<DuckDBParameter>());
    
    public void AddRange(IEnumerable<DuckDBParameter> values)
        => parameters.AddRange(values);

    private int IndexOfSafe(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found");
        return index;
    }
}
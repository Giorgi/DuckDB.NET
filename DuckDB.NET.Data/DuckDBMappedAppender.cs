using System;
using System.Collections.Generic;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Mapping;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

/// <summary>
/// A type-safe appender that uses ClassMap to validate type mappings.
/// </summary>
/// <typeparam name="T">The type being appended</typeparam>
/// <typeparam name="TMap">The ClassMap type defining the mappings</typeparam>
public class DuckDBMappedAppender<T, TMap> : IDisposable where TMap : DuckDBClassMap<T>, new()
{
    private readonly DuckDBAppender appender;
    private readonly List<IPropertyMapping<T>> mappings;

    internal DuckDBMappedAppender(DuckDBAppender appender)
    {
        this.appender = appender;
        var classMap = new TMap();

        // Get mappings as List<T> to avoid interface enumerator boxing
        mappings = classMap.PropertyMappings;

        // Validate mappings match the table structure
        if (mappings.Count == 0)
        {
            throw new InvalidOperationException($"ClassMap {typeof(TMap).Name} has no property mappings defined");
        }

        var columnTypes = appender.LogicalTypes;
        if (mappings.Count != columnTypes.Count)
        {
            throw new InvalidOperationException($"ClassMap {typeof(TMap).Name} has {mappings.Count} mappings but table has {columnTypes.Count} columns");
        }

        for (int index = 0; index < mappings.Count; index++)
        {
            var mapping = mappings[index];

            if (mapping.MappingType != PropertyMappingType.Property)
            {
                continue;
            }

            var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(columnTypes[index]);
            var expectedType = GetExpectedDuckDBType(mapping.PropertyType);

            if (expectedType != columnType)
            {
                throw new InvalidOperationException(
                    $"Type mismatch at column index {index}: Mapped type is {mapping.PropertyType.Name} (expected DuckDB type: {expectedType}) but actual column type is {columnType}");
            }
        }
    }

    /// <summary>
    /// Appends multiple records to the table.
    /// </summary>
    /// <param name="records">The records to append</param>
    public void AppendRecords(IEnumerable<T> records)
    {
        if (records == null)
        {
            throw new ArgumentNullException(nameof(records));
        }

        foreach (var record in records)
        {
            AppendRecord(record);
        }
    }

    private void AppendRecord(T record)
    {
        if (record == null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        var row = appender.CreateRow();

        foreach (var mapping in mappings)
        {
            row = mapping.AppendToRow(row, record);
        }

        row.EndRow();
    }

    private static DuckDBType GetExpectedDuckDBType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        var duckDBType = underlyingType.GetDuckDBType();

        return duckDBType switch
        {
            DuckDBType.Invalid => throw new NotSupportedException($"Type {type.Name} is not supported for mapping"),
            _ => duckDBType
        };
    }

    /// <summary>
    /// Closes the appender and flushes any remaining data.
    /// </summary>
    public void Close()
    {
        appender.Close();
    }

    /// <summary>
    /// Disposes the appender.
    /// </summary>
    public void Dispose()
    {
        appender.Dispose();
    }
}

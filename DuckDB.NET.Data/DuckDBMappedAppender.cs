using System;
using System.Collections.Generic;
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
    private readonly TMap classMap;
    private readonly Mapping.PropertyMapping[] orderedMappings;

    internal DuckDBMappedAppender(DuckDBAppender appender)
    {
        this.appender = appender;
        this.classMap = new TMap();
        
        // Validate mappings match the table structure
        var mappings = classMap.PropertyMappings;
        if (mappings.Count == 0)
        {
            throw new InvalidOperationException($"ClassMap {typeof(TMap).Name} has no property mappings defined");
        }

        // Order mappings by column index
        orderedMappings = new Mapping.PropertyMapping[mappings.Count];
        for (int i = 0; i < mappings.Count; i++)
        {
            var mapping = mappings[i];
            var columnIndex = mapping.ColumnIndex ?? i;
            
            if (columnIndex < 0 || columnIndex >= mappings.Count)
            {
                throw new InvalidOperationException($"Invalid column index {columnIndex} for property {mapping.PropertyName}");
            }

            orderedMappings[columnIndex] = mapping;
        }
    }

    /// <summary>
    /// Appends a single record to the table.
    /// </summary>
    /// <param name="record">The record to append</param>
    public void AppendRecord(T record)
    {
        if (record == null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        var row = appender.CreateRow();

        foreach (var mapping in orderedMappings)
        {
            var value = mapping.Getter(record);
            AppendValue(row, value, mapping.PropertyType);
        }

        row.EndRow();
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

    private static void AppendValue(IDuckDBAppenderRow row, object? value, Type propertyType)
    {
        if (value == null)
        {
            row.AppendNullValue();
            return;
        }

        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        switch (value)
        {
            case bool boolValue:
                row.AppendValue(boolValue);
                break;
            case sbyte sbyteValue:
                row.AppendValue((sbyte?)sbyteValue);
                break;
            case short shortValue:
                row.AppendValue((short?)shortValue);
                break;
            case int intValue:
                row.AppendValue((int?)intValue);
                break;
            case long longValue:
                row.AppendValue((long?)longValue);
                break;
            case byte byteValue:
                row.AppendValue((byte?)byteValue);
                break;
            case ushort ushortValue:
                row.AppendValue((ushort?)ushortValue);
                break;
            case uint uintValue:
                row.AppendValue((uint?)uintValue);
                break;
            case ulong ulongValue:
                row.AppendValue((ulong?)ulongValue);
                break;
            case float floatValue:
                row.AppendValue((float?)floatValue);
                break;
            case double doubleValue:
                row.AppendValue((double?)doubleValue);
                break;
            case decimal decimalValue:
                row.AppendValue((decimal?)decimalValue);
                break;
            case string stringValue:
                row.AppendValue(stringValue);
                break;
            case DateTime dateTimeValue:
                row.AppendValue((DateTime?)dateTimeValue);
                break;
            case DateTimeOffset dateTimeOffsetValue:
                row.AppendValue((DateTimeOffset?)dateTimeOffsetValue);
                break;
            case TimeSpan timeSpanValue:
                row.AppendValue((TimeSpan?)timeSpanValue);
                break;
            case Guid guidValue:
                row.AppendValue((Guid?)guidValue);
                break;
#if NET6_0_OR_GREATER
            case DateOnly dateOnlyValue:
                row.AppendValue((DateOnly?)dateOnlyValue);
                break;
            case TimeOnly timeOnlyValue:
                row.AppendValue((TimeOnly?)timeOnlyValue);
                break;
#endif
            default:
                throw new NotSupportedException($"Type {value.GetType().Name} is not supported for appending");
        }
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

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
    private readonly PropertyMapping<T>[] orderedMappings;

    internal DuckDBMappedAppender(DuckDBAppender appender)
    {
        this.appender = appender;
        classMap = new TMap();
        
        // Validate mappings match the table structure
        var mappings = classMap.PropertyMappings;
        if (mappings.Count == 0)
        {
            throw new InvalidOperationException($"ClassMap {typeof(TMap).Name} has no property mappings defined");
        }

        var columnTypes = appender.LogicalTypes;
        if (mappings.Count != columnTypes.Count)
        {
            throw new InvalidOperationException($"ClassMap {typeof(TMap).Name} has {mappings.Count} mappings but table has {columnTypes.Count} columns");
        }

        // Validate each mapping
        orderedMappings = new PropertyMapping<T>[mappings.Count];
        for (int i = 0; i < mappings.Count; i++)
        {
            var mapping = mappings[i];
            orderedMappings[i] = mapping;

            // Skip validation for Default and Null mappings
            if (mapping.MappingType != PropertyMappingType.Property)
            {
                continue;
            }

            // Get the actual column type from the appender
            var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(columnTypes[i]);
            var expectedType = GetExpectedDuckDBType(mapping.PropertyType);

            if (expectedType != columnType)
            {
                throw new InvalidOperationException(
                    $"Type mismatch for property '{mapping.PropertyName}': " +
                    $"Property type is {mapping.PropertyType.Name} (maps to {expectedType}) " +
                    $"but column {i} is {columnType}");
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

        foreach (var mapping in orderedMappings)
        {
            switch (mapping.MappingType)
            {
                case PropertyMappingType.Property:
                    var value = mapping.Getter(record);
                    AppendValue(row, value);
                    break;
                case PropertyMappingType.Default:
                    row.AppendDefault();
                    break;
                case PropertyMappingType.Null:
                    row.AppendNullValue();
                    break;
            }
        }

        row.EndRow();
    }

    private static DuckDBType GetExpectedDuckDBType(Type type)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType switch
        {
            Type t when t == typeof(bool) => DuckDBType.Boolean,
            Type t when t == typeof(sbyte) => DuckDBType.TinyInt,
            Type t when t == typeof(short) => DuckDBType.SmallInt,
            Type t when t == typeof(int) => DuckDBType.Integer,
            Type t when t == typeof(long) => DuckDBType.BigInt,
            Type t when t == typeof(byte) => DuckDBType.UnsignedTinyInt,
            Type t when t == typeof(ushort) => DuckDBType.UnsignedSmallInt,
            Type t when t == typeof(uint) => DuckDBType.UnsignedInteger,
            Type t when t == typeof(ulong) => DuckDBType.UnsignedBigInt,
            Type t when t == typeof(float) => DuckDBType.Float,
            Type t when t == typeof(double) => DuckDBType.Double,
            Type t when t == typeof(decimal) => DuckDBType.Decimal,
            Type t when t == typeof(string) => DuckDBType.Varchar,
            Type t when t == typeof(DateTime) => DuckDBType.Timestamp,
            Type t when t == typeof(DateTimeOffset) => DuckDBType.TimestampTz,
            Type t when t == typeof(TimeSpan) => DuckDBType.Interval,
            Type t when t == typeof(Guid) => DuckDBType.Uuid,
#if NET6_0_OR_GREATER
            Type t when t == typeof(DateOnly) => DuckDBType.Date,
            Type t when t == typeof(TimeOnly) => DuckDBType.Time,
#endif
            _ => throw new NotSupportedException($"Type {type.Name} is not supported for mapping")
        };
    }

    private static void AppendValue(IDuckDBAppenderRow row, object? value)
    {
        if (value == null)
        {
            row.AppendNullValue();
            return;
        }

        switch (value)
        {
            case bool boolValue:
                row.AppendValue(boolValue);
                break;
            case sbyte sbyteValue:
                row.AppendValue(sbyteValue);
                break;
            case short shortValue:
                row.AppendValue(shortValue);
                break;
            case int intValue:
                row.AppendValue(intValue);
                break;
            case long longValue:
                row.AppendValue(longValue);
                break;
            case byte byteValue:
                row.AppendValue(byteValue);
                break;
            case ushort ushortValue:
                row.AppendValue(ushortValue);
                break;
            case uint uintValue:
                row.AppendValue(uintValue);
                break;
            case ulong ulongValue:
                row.AppendValue(ulongValue);
                break;
            case float floatValue:
                row.AppendValue(floatValue);
                break;
            case double doubleValue:
                row.AppendValue(doubleValue);
                break;
            case decimal decimalValue:
                row.AppendValue(decimalValue);
                break;
            case string stringValue:
                row.AppendValue(stringValue);
                break;
            case DateTime dateTimeValue:
                row.AppendValue(dateTimeValue);
                break;
            case DateTimeOffset dateTimeOffsetValue:
                row.AppendValue(dateTimeOffsetValue);
                break;
            case TimeSpan timeSpanValue:
                row.AppendValue(timeSpanValue);
                break;
            case Guid guidValue:
                row.AppendValue(guidValue);
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

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
    private readonly TMap classMap;

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
                    $"Type mismatch for property '{mapping.PropertyName}': " +
                    $"Property type is {mapping.PropertyType.Name} (maps to {expectedType}) " +
                    $"but column {index} is {columnType}");
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

        foreach (var mapping in classMap.PropertyMappings)
        {
            _ = mapping.MappingType switch
            {
                PropertyMappingType.Property => AppendValue(row, mapping.Getter(record)),
                PropertyMappingType.Default => row.AppendDefault(),
                PropertyMappingType.Null => row.AppendNullValue(),
                _ => row
            };
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

    private static IDuckDBAppenderRow AppendValue(IDuckDBAppenderRow row, object? value)
    {
        if (value == null)
        {
            return row.AppendNullValue();
        }

        return value switch
        {
            bool boolValue => row.AppendValue(boolValue),
            sbyte sbyteValue => row.AppendValue(sbyteValue),
            short shortValue => row.AppendValue(shortValue),
            int intValue => row.AppendValue(intValue),
            long longValue => row.AppendValue(longValue),
            byte byteValue => row.AppendValue(byteValue),
            ushort ushortValue => row.AppendValue(ushortValue),
            uint uintValue => row.AppendValue(uintValue),
            ulong ulongValue => row.AppendValue(ulongValue),
            float floatValue => row.AppendValue(floatValue),
            double doubleValue => row.AppendValue(doubleValue),
            decimal decimalValue => row.AppendValue(decimalValue),
            string stringValue => row.AppendValue(stringValue),
            DateTime dateTimeValue => row.AppendValue(dateTimeValue),
            DateTimeOffset dateTimeOffsetValue => row.AppendValue(dateTimeOffsetValue),
            TimeSpan timeSpanValue => row.AppendValue(timeSpanValue),
            Guid guidValue => row.AppendValue(guidValue),
#if NET6_0_OR_GREATER
            DateOnly dateOnlyValue => row.AppendValue((DateOnly?)dateOnlyValue),
            TimeOnly timeOnlyValue => row.AppendValue((TimeOnly?)timeOnlyValue),
#endif
            _ => throw new NotSupportedException($"Type {value.GetType().Name} is not supported for appending")
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

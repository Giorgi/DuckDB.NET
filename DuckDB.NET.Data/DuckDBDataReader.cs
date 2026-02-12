using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;

namespace DuckDB.NET.Data;

public class DuckDBDataReader : DbDataReader
{
    private readonly DuckDBCommand command;
    private readonly CommandBehavior behavior;

    private DuckDBResult currentResult;
    private DuckDBDataChunk? currentChunk;

    private int fieldCount;

    private ulong currentChunkRowCount;
    private ulong rowsReadFromCurrentChunk;

    private bool closed;
    private bool hasRows;
    private bool streamingResult;
    private long currentChunkIndex;

    private readonly IEnumerator<DuckDBResult> resultEnumerator;
    private VectorDataReaderBase[] vectorReaders = [];
    private Dictionary<string, int> columnMapping = [];

    internal DuckDBDataReader(DuckDBCommand command, IEnumerable<DuckDBResult> queryResults, CommandBehavior behavior)
    {
        this.command = command;
        this.behavior = behavior;
        resultEnumerator = queryResults.GetEnumerator();

        InitNextReader();
    }

    private bool InitNextReader()
    {
        while (resultEnumerator.MoveNext())
        {
            var result = resultEnumerator.Current;
            if (NativeMethods.Query.DuckDBResultReturnType(result) == DuckDBResultType.QueryResult)
            {
                currentChunkIndex = 0;
                currentResult = result;

                columnMapping = [];
                fieldCount = (int)NativeMethods.Query.DuckDBColumnCount(ref currentResult);
                streamingResult = NativeMethods.Types.DuckDBResultIsStreaming(currentResult) > 0;

                hasRows = InitChunkData();

                return true;
            }

            result.Close();
        }

        return false;
    }

    private bool InitChunkData()
    {
        foreach (var reader in vectorReaders)
        {
            reader.Dispose();
        }

        currentChunk?.Dispose();
        currentChunk = streamingResult ? NativeMethods.StreamingResult.DuckDBStreamFetchChunk(currentResult) : NativeMethods.Types.DuckDBResultGetChunk(currentResult, currentChunkIndex);

        rowsReadFromCurrentChunk = 0;

        currentChunkRowCount = NativeMethods.DataChunks.DuckDBDataChunkGetSize(currentChunk);

        if (vectorReaders.Length != fieldCount)
        {
            vectorReaders = new VectorDataReaderBase[fieldCount];
        }

        for (int index = 0; index < fieldCount; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(currentChunk, index);

            using var logicalType = NativeMethods.Query.DuckDBColumnLogicalType(ref currentResult, index);

            var columnName = vectorReaders[index]?.ColumnName ?? NativeMethods.Query.DuckDBColumnName(ref currentResult, index).ToManagedString(false);
            vectorReaders[index] = VectorDataReaderFactory.CreateReader(vector, logicalType, columnName);
        }

        if (columnMapping.Count == 0)
        {
            for (var i = 0; i < vectorReaders.Length; i++)
            {
                if (!columnMapping.ContainsKey(vectorReaders[i].ColumnName))
                {
                    columnMapping.Add(vectorReaders[i].ColumnName, i);
                }
            }
        }

        return currentChunkRowCount > 0;
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetFieldValue<bool>(ordinal);
    }

    public override byte GetByte(int ordinal)
    {
        return GetFieldValue<byte>(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotSupportedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return vectorReaders[ordinal].DuckDBType.ToString();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        return GetFieldValue<DateTime>(ordinal);
    }

    public override decimal GetDecimal(int ordinal)
    {
        return GetFieldValue<decimal>(ordinal);
    }

    public override double GetDouble(int ordinal)
    {
        return GetFieldValue<double>(ordinal);
    }

    public override Type GetFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ClrType;
    }

    public override Type GetProviderSpecificFieldType(int ordinal)
    {
        return vectorReaders[ordinal].ProviderSpecificClrType;
    }

    public override float GetFloat(int ordinal)
    {
        return GetFieldValue<float>(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        return GetFieldValue<Guid>(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        return GetFieldValue<short>(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        return GetFieldValue<int>(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        return GetFieldValue<long>(ordinal);
    }

    public override string GetName(int ordinal)
    {
        return vectorReaders[ordinal].ColumnName;
    }

    public override int GetOrdinal(string name)
    {
        if (columnMapping.TryGetValue(name, out var index))
        {
            return index;
        }

        throw new DuckDBException($"Column with name {name} was not found.");
    }

    public override string GetString(int ordinal)
    {
        return GetFieldValue<string>(ordinal);
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        CheckRowRead();

        return vectorReaders[ordinal].GetValue<T>(rowsReadFromCurrentChunk - 1);
    }

    public override object GetValue(int ordinal)
    {
        CheckRowRead();

        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetValue(rowsReadFromCurrentChunk - 1);
    }

    public override object GetProviderSpecificValue(int ordinal)
    {
        CheckRowRead();

        return IsDBNull(ordinal) ? DBNull.Value : vectorReaders[ordinal].GetProviderSpecificValue(rowsReadFromCurrentChunk - 1);
    }

    public override int GetValues(object[] values)
    {
        for (var i = 0; i < FieldCount; i++)
        {
            values[i] = GetValue(i);
        }

        return FieldCount;
    }

    public override Stream GetStream(int ordinal)
    {
        return GetFieldValue<Stream>(ordinal);
    }

    public override bool IsDBNull(int ordinal)
    {
        CheckRowRead();

        return !vectorReaders[ordinal].IsValid(rowsReadFromCurrentChunk - 1);
    }

    public override int FieldCount => fieldCount;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int RecordsAffected => -1;

    public override bool HasRows => hasRows;

    public override bool IsClosed => closed;

    public override bool NextResult()
    {
        return InitNextReader();
    }

    public override bool Read()
    {
        if (rowsReadFromCurrentChunk == currentChunkRowCount)
        {
            currentChunkIndex++;
            var hasData = InitChunkData();

            if (hasData)
            {
                rowsReadFromCurrentChunk++;
            }

            return hasData;
        }
        else
        {
            rowsReadFromCurrentChunk++;

            return true;
        }
    }

    public override int Depth { get; }

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this, behavior == CommandBehavior.CloseConnection);
    }

    public override DataTable GetSchemaTable()
    {
        var table = new DataTable
        {
            Columns =
            {
                { SchemaTableColumn.ColumnName, typeof(string) },
                { SchemaTableColumn.ColumnOrdinal, typeof(int) },
                { SchemaTableColumn.ColumnSize, typeof(int) },
                { SchemaTableColumn.NumericPrecision, typeof(byte)},
                { SchemaTableColumn.NumericScale, typeof(byte) },
                { SchemaTableColumn.DataType, typeof(Type) },
                { SchemaTableColumn.AllowDBNull, typeof(bool)  },
                { SchemaTableColumn.BaseSchemaName, typeof(string) },
                { SchemaTableColumn.BaseTableName, typeof(string) },
                { SchemaTableColumn.BaseColumnName, typeof(string) }
            }
        };

        // Get table names from the query
        // Note: DuckDB's duckdb_get_table_names returns unique table names referenced in the query,
        // not per-column mappings. For single-table queries, we can populate BaseTableName.
        // For multi-table queries (joins), the mapping is not directly available from the C API.
        var tableNames = GetTableNamesFromQuery();
        var singleTableName = tableNames != null && tableNames.Length == 1 ? tableNames[0] : null;

        var rowData = new object[10];

        for (var i = 0; i < FieldCount; i++)
        {
            var columnName = GetName(i);
            rowData[0] = columnName;
            rowData[1] = i;
            rowData[2] = -1;
            rowData[5] = GetFieldType(i);
            rowData[6] = true;

            if (vectorReaders[i] is DecimalVectorDataReader decimalVectorDataReader)
            {
                rowData[4] = decimalVectorDataReader.Scale;
                rowData[3] = decimalVectorDataReader.Precision;
            }
            else
            {
                rowData[3] = rowData[4] = DBNull.Value;
            }

            // Set table name information
            // For single-table queries, populate the table name
            // For multi-table queries, BaseTableName will be DBNull since we cannot determine
            // which table each column comes from without additional API support
            if (!string.IsNullOrEmpty(singleTableName))
            {
                // The table name from duckdb_get_table_names with qualified=true should be in the format "schema.table"
                // Split by the last dot to handle schema names or table names that might contain dots
                var lastDotIndex = singleTableName.LastIndexOf('.');
                if (lastDotIndex > 0 && lastDotIndex < singleTableName.Length - 1)
                {
                    rowData[7] = singleTableName.Substring(0, lastDotIndex); // BaseSchemaName
                    rowData[8] = singleTableName.Substring(lastDotIndex + 1); // BaseTableName
                }
                else
                {
                    // No schema qualifier found, just use the table name
                    rowData[7] = DBNull.Value; // BaseSchemaName
                    rowData[8] = singleTableName; // BaseTableName
                }
            }
            else
            {
                rowData[7] = DBNull.Value;
                rowData[8] = DBNull.Value;
            }

            rowData[9] = columnName; // BaseColumnName

            table.Rows.Add(rowData);
        }

        return table;
    }

    private string[]? GetTableNamesFromQuery()
    {
        try
        {
            var duckDBConnection = command?.Connection as DuckDBConnection;
            if (duckDBConnection?.NativeConnection == null || command?.CommandText == null || string.IsNullOrEmpty(command.CommandText))
            {
                return null;
            }

            // Call duckdb_get_table_names with qualified=true to get schema-qualified names
            using var tableNamesValue = NativeMethods.Query.DuckDBGetTableNames(
                duckDBConnection.NativeConnection,
                command.CommandText,
                true);

            if (tableNamesValue.IsNull())
            {
                return null;
            }

            // Get the size of the list
            var listSize = NativeMethods.Value.DuckDBGetListSize(tableNamesValue);
            
            // If the list is empty, return null
            if (listSize == 0)
            {
                return null;
            }

            var tableNames = new string[listSize];

            // Extract each table name from the list
            for (ulong i = 0; i < listSize; i++)
            {
                using var childValue = NativeMethods.Value.DuckDBGetListChild(tableNamesValue, i);
                if (!childValue.IsNull())
                {
                    tableNames[i] = NativeMethods.Value.DuckDBGetVarchar(childValue);
                }
                else
                {
                    tableNames[i] = string.Empty;
                }
            }

            return tableNames;
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or InvalidOperationException)
        {
            // If we fail to get table names due to missing DLL, missing entry point, or operation errors,
            // just return null. This ensures backward compatibility - if the feature isn't available or fails,
            // we just don't populate the table names.
            // We don't log here to avoid noise in normal operation when the feature might not be available.
            return null;
        }
    }

    public override void Close()
    {
        if (closed) return;

        foreach (var reader in vectorReaders)
        {
            reader.Dispose();
        }

        currentChunk?.Dispose();
        currentResult.Close();

        if (behavior == CommandBehavior.CloseConnection)
        {
            command.CloseConnection();
        }

        closed = true;
        resultEnumerator.Dispose();
    }

    private void CheckRowRead()
    {
        if (rowsReadFromCurrentChunk <= 0)
        {
            throw new InvalidOperationException("No row has been read");
        }
    }
}